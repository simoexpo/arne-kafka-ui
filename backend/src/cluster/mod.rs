pub mod admin;
pub mod assignment;
pub mod ffi;
pub mod group_lag_cache;
pub mod keyed_cache;
pub mod registry;
pub mod sampler;
pub mod single_flight;
pub mod snapshot;

use crate::config::{ClusterConfig, SaslMechanism};
use crate::message::schema_registry::SchemaRegistry;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::ClientConfig;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
/// A detail page nobody has opened for this long is forgotten.
pub const DETAIL_HORIZON_MS: i64 = 600_000;
/// Just under the detail pages' 10s poll.
pub const DETAIL_TTL_MS: i64 = 8_000;

/// Deliberately SHORT next to the other snapshots: health is the cheapest
/// call we make (one DescribeCluster) and the one users most need current, so
/// this window only exists to collapse simultaneous polls from several tabs —
/// never to delay noticing that a cluster went down.
pub const HEALTH_TTL_MS: i64 = 2_000;

/// Consecutive health-check failures before probing whether a freshly built
/// client can reach the cluster the resident one cannot.
pub const RECOVERY_THRESHOLD: u32 = 2;

pub fn build_client_config(cfg: &ClusterConfig) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", &cfg.bootstrap);
    cc.set("allow.auto.create.topics", "false");
    cc.set("socket.keepalive.enable", "true");
    match &cfg.sasl {
        None => { cc.set("security.protocol", "plaintext"); }
        Some(s) => {
            cc.set("security.protocol", if s.tls { "sasl_ssl" } else { "sasl_plaintext" });
            cc.set("sasl.mechanism", match s.mechanism {
                SaslMechanism::Plain => "PLAIN",
                SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
                SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            });
            cc.set("sasl.username", &s.username);
            cc.set("sasl.password", &s.password);
        }
    }
    cc
}

/// Monotonic per-process counter backing `throwaway_group_id`, shared across
/// every call site that mints one.
static THROWAWAY_SEQ: AtomicU64 = AtomicU64::new(0);

/// Mints a throwaway consumer group.id for pure assign()-based consumption
/// with no group management involved (a bounded fetch, a live tail):
/// librdkafka's consumer machinery requires a group.id even though no real
/// consumer group is ever joined, subscribed to, or committed against.
/// `kind` (e.g. "fetch", "tail") only distinguishes call sites for
/// debugging; the id itself is otherwise arbitrary and never reused.
pub fn throwaway_group_id(kind: &str) -> String {
    let seq = THROWAWAY_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("arne-{kind}-{}-{}-{seq}", std::process::id(), crate::util::now_ms())
}

pub struct ClusterHandle {
    pub name: String,
    pub config: ClusterConfig,
    pub sampler: Arc<sampler::SamplerStore>,
    /// One throughput sample per topic at a time, and one lag refresh per
    /// (topic, group) at a time: concurrent pollers wait for the in-flight
    /// refresh instead of duplicating it (owner design 2026-08-19).
    pub sampler_flight: single_flight::SingleFlight<String>,
    /// Lag for ONE topic's partitions, as that topic's tab shows it. Carries
    /// the two-tier policy, because "this group has no offsets on this topic"
    /// is a fact worth remembering for a while.
    pub group_lag_cache: group_lag_cache::GroupLagCache,
    pub lag_flight: single_flight::SingleFlight<(String, String)>,
    /// Lag across every topic a group reads, as the consumers list shows it.
    /// A separate cache, not a scope inside the one above: different question,
    /// different key, different freshness policy (owner ruling 2026-08-20).
    pub cluster_lag_cache: keyed_cache::KeyedCache<String, Vec<admin::PartitionLag>>,
    pub cluster_lag_flight: single_flight::SingleFlight<String>,
    /// Whole-response snapshots shared by every tab for a few seconds, so tab
    /// count stops multiplying broker calls (owner design 2026-08-19).
    pub topics_snapshot: snapshot::SnapshotCache<admin::TopicList>,
    pub overview_snapshot: snapshot::SnapshotCache<admin::Overview>,
    pub groups_snapshot: snapshot::SnapshotCache<admin::GroupList>,
    pub health_snapshot: snapshot::SnapshotCache<ClusterHealth>,
    pub snapshot_flight: single_flight::SingleFlight<&'static str>,
    /// Per-entity answers: one topic's partitions+config, one group's detail.
    /// Keyed, because a single slot would serve one topic's data under
    /// another's name (owner ruling 2026-08-20).
    pub topic_detail_cache: keyed_cache::KeyedCache<String, admin::TopicDetail>,
    pub group_detail_cache: keyed_cache::KeyedCache<String, admin::GroupDetail>,
    pub detail_flight: Arc<single_flight::SingleFlight<(&'static str, String)>>,
    pub schema_registry: Option<Arc<SchemaRegistry>>,
    consumer: RwLock<Arc<BaseConsumer>>,
    admin: RwLock<Arc<AdminClient<DefaultClientContext>>>,
    pub(crate) health_failures: AtomicU32,
    pub(crate) probe_in_flight: AtomicBool,
}

impl std::fmt::Debug for ClusterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHandle").field("name", &self.name).finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus { Healthy, Unreachable }

#[derive(Debug, Serialize, Clone)]
pub struct ClusterHealth {
    pub status: HealthStatus,
    pub broker_count: Option<usize>,
    pub error: Option<String>,
}

impl ClusterHandle {
    pub fn connect(config: ClusterConfig) -> KafkaResult<Self> {
        let cc = build_client_config(&config);
        let consumer: BaseConsumer = cc.create()?;
        let admin: AdminClient<DefaultClientContext> = cc.create()?;
        let schema_registry = config.schema_registry.as_ref().map(|sr| Arc::new(SchemaRegistry::new(&sr.url)));
        Ok(Self {
            name: config.name.clone(),
            config,
            sampler: Arc::new(sampler::SamplerStore::new()),
            sampler_flight: single_flight::SingleFlight::new(),
            group_lag_cache: group_lag_cache::GroupLagCache::new(),
            lag_flight: single_flight::SingleFlight::new(),
            cluster_lag_cache: keyed_cache::KeyedCache::new(group_lag_cache::EVICT_AGE_MS),
            cluster_lag_flight: single_flight::SingleFlight::new(),
            topics_snapshot: snapshot::SnapshotCache::new(),
            overview_snapshot: snapshot::SnapshotCache::new(),
            groups_snapshot: snapshot::SnapshotCache::new(),
            health_snapshot: snapshot::SnapshotCache::new(),
            snapshot_flight: single_flight::SingleFlight::new(),
            topic_detail_cache: keyed_cache::KeyedCache::new(DETAIL_HORIZON_MS),
            group_detail_cache: keyed_cache::KeyedCache::new(DETAIL_HORIZON_MS),
            detail_flight: Arc::new(single_flight::SingleFlight::new()),
            schema_registry,
            consumer: RwLock::new(Arc::new(consumer)),
            admin: RwLock::new(Arc::new(admin)),
            health_failures: AtomicU32::new(0),
            probe_in_flight: AtomicBool::new(false),
        })
    }

    pub fn consumer(&self) -> Arc<BaseConsumer> {
        self.consumer.read().expect("consumer lock poisoned").clone()
    }

    pub fn admin(&self) -> Arc<AdminClient<DefaultClientContext>> {
        self.admin.read().expect("admin lock poisoned").clone()
    }

    fn swap_clients(&self, consumer: BaseConsumer, admin: AdminClient<DefaultClientContext>) {
        // The two slots swap under separate locks, so a concurrent reader can
        // briefly observe new-consumer + old-admin. Both are built from the
        // same config; worst case is one bounded call on the outgoing client.
        let old_consumer = std::mem::replace(
            &mut *self.consumer.write().expect("consumer lock poisoned"),
            Arc::new(consumer),
        );
        let old_admin = std::mem::replace(
            &mut *self.admin.write().expect("admin lock poisoned"),
            Arc::new(admin),
        );
        // Dropped here, after both locks are released: rd_kafka_destroy can
        // stall, and it must never run while readers block on the lock.
        drop(old_consumer);
        drop(old_admin);
    }

    /// Test hook: rebuild the resident client pair against a different
    /// bootstrap while `self.config` keeps the real one — simulates a stale
    /// resident client over a healthy path (the wedge this module heals).
    #[doc(hidden)]
    pub fn replace_clients_with_bootstrap(&self, bootstrap: &str) -> KafkaResult<()> {
        let mut cfg = self.config.clone();
        cfg.bootstrap = bootstrap.to_string();
        let cc = build_client_config(&cfg);
        let consumer: BaseConsumer = cc.create()?;
        let admin: AdminClient<DefaultClientContext> = cc.create()?;
        self.swap_clients(consumer, admin);
        Ok(())
    }

    /// DescribeCluster, not metadata (owner ruling 2026-08-19): the nav polls
    /// this for every cluster every 10s, and `fetch_metadata(None)` answers
    /// "is it up, how many brokers" by shipping every partition of every
    /// topic — megabytes on a large cluster, for a dot and a number.
    pub async fn health(self: &Arc<Self>) -> ClusterHealth {
        let this = self.clone();
        // Shared across tabs: the nav polls every cluster every 10s from every
        // open tab, and the answer is identical for all of them.
        let res = tokio::task::spawn_blocking(move || {
            snapshot::cached_or_refresh(
                &this.health_snapshot,
                &this.snapshot_flight,
                "health",
                HEALTH_TTL_MS,
                || Ok(this.health_blocking()),
            )
        })
        .await;
        match res {
            Ok(Ok(health)) => health,
            Ok(Err(e)) => ClusterHealth { status: HealthStatus::Unreachable, broker_count: None, error: Some(e.message) },
            Err(e) => ClusterHealth { status: HealthStatus::Unreachable, broker_count: None, error: Some(e.to_string()) },
        }
    }

    /// Infallible by design: "unreachable" IS the answer, and the snapshot
    /// above caches that answer like any other.
    ///
    /// Public as a test hook so the self-heal mechanism can be exercised
    /// probe-by-probe, below the shared snapshot.
    #[doc(hidden)]
    pub fn health_blocking(&self) -> ClusterHealth {
        match ffi::describe_cluster_blocking(self, HEALTH_TIMEOUT) {
            Ok(described) => {
                self.reset_shared_failures();
                ClusterHealth {
                    status: HealthStatus::Healthy,
                    broker_count: Some(described.brokers.len()),
                    error: None,
                }
            }
            Err(e) => match self.note_shared_failure_and_maybe_recover() {
                // A fresh client reached the cluster the resident one could not.
                Some(brokers) => ClusterHealth {
                    status: HealthStatus::Healthy,
                    broker_count: Some(brokers),
                    error: None,
                },
                None => ClusterHealth {
                    status: HealthStatus::Unreachable,
                    broker_count: None,
                    error: Some(e.message),
                },
            },
        }
    }

    /// Record one failed shared-client operation; returns the new consecutive
    /// count. Saturating: wraparound would silently skip a recovery cycle,
    /// saturation just keeps probing at every subsequent check.
    pub(crate) fn record_shared_failure(&self) -> u32 {
        self.health_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| Some(n.saturating_add(1)))
            .expect("closure never returns None")
            .saturating_add(1)
    }

    pub(crate) fn reset_shared_failures(&self) {
        self.health_failures.store(0, Ordering::SeqCst);
    }

    /// A shared-client operation failed: count it and, at the threshold, probe
    /// whether a freshly built client can reach the cluster — healing if so.
    /// Returns the fresh broker count only on a real heal. Blocking (the probe
    /// does network I/O); call only from blocking contexts.
    pub(crate) fn note_shared_failure_and_maybe_recover(&self) -> Option<usize> {
        if self.record_shared_failure() >= RECOVERY_THRESHOLD {
            self.try_recover()
        } else {
            None
        }
    }

    /// The resident client keeps failing; find out whether the path itself is
    /// dead or only the client is. Build a fresh pair from config, probe it, and
    /// swap it in ONLY on a successful probe — so recovery can never mask a real
    /// outage. Single-flight; runs on the blocking pool (caller is inside
    /// spawn_blocking). Returns the fresh broker count when it healed.
    fn try_recover(&self) -> Option<usize> {
        if self.probe_in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        // Guarantees the flag is released on every exit path, including an
        // unwinding panic inside the closure below (e.g. a poisoned lock in
        // `swap_clients`) — without it, a panic here would latch
        // `probe_in_flight` at `true` forever, silently disabling self-heal.
        let _guard = ProbeGuard(&self.probe_in_flight);
        let cc = build_client_config(&self.config);
        let consumer: BaseConsumer = cc.create().ok()?;
        let brokers = consumer.fetch_metadata(None, HEALTH_TIMEOUT).ok()?.brokers().len();
        let admin: AdminClient<DefaultClientContext> = cc.create().ok()?;
        self.swap_clients(consumer, admin);
        self.health_failures.store(0, Ordering::SeqCst);
        tracing::warn!(cluster = %self.name, "stale kafka connection replaced with a fresh one");
        Some(brokers)
    }
}

/// Releases `probe_in_flight` when dropped — on a normal return, an early
/// `?` bailout, or an unwinding panic alike, so a single-flight probe can
/// never be left permanently latched.
struct ProbeGuard<'a>(&'a AtomicBool);

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, SaslConfig, SaslMechanism};

    fn base(name: &str) -> ClusterConfig {
        ClusterConfig { name: name.into(), bootstrap: "b:9092".into(), sasl: None, schema_registry: None }
    }

    fn base_with_bootstrap(bootstrap: &str) -> ClusterConfig {
        ClusterConfig { name: "a".into(), bootstrap: bootstrap.into(), sasl: None, schema_registry: None }
    }

    #[test]
    fn plaintext_when_no_sasl() {
        let cc = build_client_config(&base("a"));
        assert_eq!(cc.get("bootstrap.servers"), Some("b:9092"));
        assert_eq!(cc.get("security.protocol"), Some("plaintext"));
        assert_eq!(cc.get("allow.auto.create.topics"), Some("false"));
    }

    #[test]
    fn sasl_scram_over_tls() {
        let mut cfg = base("a");
        cfg.sasl = Some(SaslConfig {
            mechanism: SaslMechanism::ScramSha512,
            username: "u".into(), password: "p".into(), tls: true,
        });
        let cc = build_client_config(&cfg);
        assert_eq!(cc.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(cc.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(cc.get("sasl.username"), Some("u"));
        assert_eq!(cc.get("sasl.password"), Some("p"));
    }

    #[test]
    fn sasl_plain_without_tls() {
        let mut cfg = base("a");
        cfg.sasl = Some(SaslConfig {
            mechanism: SaslMechanism::Plain,
            username: "u".into(), password: "p".into(), tls: false,
        });
        let cc = build_client_config(&cfg);
        assert_eq!(cc.get("security.protocol"), Some("sasl_plaintext"));
        assert_eq!(cc.get("sasl.mechanism"), Some("PLAIN"));
    }

    #[test]
    fn keepalive_enabled_on_every_client() {
        let cc = build_client_config(&base("a"));
        assert_eq!(cc.get("socket.keepalive.enable"), Some("true"));
    }

    #[test]
    fn swap_replaces_client_identity() {
        let handle = ClusterHandle::connect(base("a")).expect("lazy create");
        let before = handle.consumer();
        handle.replace_clients_with_bootstrap("127.0.0.1:1").expect("lazy create");
        let after = handle.consumer();
        assert!(!Arc::ptr_eq(&before, &after), "swap must install a new client");
        // the old Arc is still usable by in-flight work
        let _still_alive: &BaseConsumer = &before;
    }

    #[test]
    fn concurrent_probe_is_single_flight() {
        let handle = ClusterHandle::connect(base("a")).expect("lazy create");
        handle.probe_in_flight.store(true, std::sync::atomic::Ordering::SeqCst);
        // a probe already in flight => try_recover declines immediately
        assert_eq!(handle.try_recover(), None);
    }

    #[test]
    fn failed_probe_releases_guard_and_keeps_resident_client() {
        // config points at a dead endpoint: probe must fail, not swap
        let handle = ClusterHandle::connect(base_with_bootstrap("127.0.0.1:1")).expect("lazy create");
        let before = handle.consumer();
        assert_eq!(handle.try_recover(), None, "probe against a dead endpoint must not heal");
        assert!(!handle.probe_in_flight.load(std::sync::atomic::Ordering::SeqCst), "guard must be released after a failed probe");
        assert!(Arc::ptr_eq(&before, &handle.consumer()), "failed probe must not swap the resident client");
    }

    #[test]
    fn failure_counter_saturates_instead_of_wrapping() {
        let handle = ClusterHandle::connect(base("a")).expect("lazy create");
        handle.health_failures.store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
        // would panic (debug) or wrap to 0 (release) with a plain fetch_add + 1
        assert_eq!(handle.record_shared_failure(), u32::MAX);
        handle.reset_shared_failures();
        assert_eq!(handle.record_shared_failure(), 1);
    }

    #[test]
    fn shared_failures_probe_only_at_threshold() {
        // config points at a dead endpoint: probes can never heal, so the only
        // observable difference between "no probe" and "failed probe" is time —
        // and the counter/identity assertions below.
        let handle = ClusterHandle::connect(base_with_bootstrap("127.0.0.1:1")).expect("lazy create");
        let before = handle.consumer();
        // below threshold: counted, no heal
        assert_eq!(handle.note_shared_failure_and_maybe_recover(), None);
        assert_eq!(handle.health_failures.load(std::sync::atomic::Ordering::SeqCst), 1);
        // at threshold: probe runs (bounded by HEALTH_TIMEOUT), fails against the
        // dead endpoint, stays honest — no swap, guard released
        assert_eq!(handle.note_shared_failure_and_maybe_recover(), None);
        assert_eq!(handle.health_failures.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!handle.probe_in_flight.load(std::sync::atomic::Ordering::SeqCst));
        assert!(Arc::ptr_eq(&before, &handle.consumer()));
    }
}
