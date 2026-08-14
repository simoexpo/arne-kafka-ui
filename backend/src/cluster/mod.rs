pub mod admin;
pub mod registry;
pub mod sampler;

use crate::config::{ClusterConfig, SaslMechanism};
use crate::message::schema_registry::SchemaRegistry;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::ClientConfig;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

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

pub fn group_consumer(cfg: &ClusterConfig, group: &str) -> KafkaResult<BaseConsumer> {
    let mut cc = build_client_config(cfg);
    cc.set("group.id", group);
    cc.create()
}

pub struct ClusterHandle {
    pub name: String,
    pub config: ClusterConfig,
    pub sampler: Arc<sampler::SamplerStore>,
    pub schema_registry: Option<Arc<SchemaRegistry>>,
    consumer: RwLock<Arc<BaseConsumer>>,
    admin: RwLock<Arc<AdminClient<DefaultClientContext>>>,
    health_failures: AtomicU32,
    probe_in_flight: AtomicBool,
}

impl std::fmt::Debug for ClusterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHandle").field("name", &self.name).finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus { Healthy, Unreachable }

#[derive(Debug, Serialize)]
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
            sampler: Arc::new(sampler::SamplerStore::new(360)),
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
        *self.consumer.write().expect("consumer lock poisoned") = Arc::new(consumer);
        *self.admin.write().expect("admin lock poisoned") = Arc::new(admin);
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

    pub async fn health(self: &Arc<Self>) -> ClusterHealth {
        let this = self.clone();
        let res = tokio::task::spawn_blocking(move || {
            match this.consumer().fetch_metadata(None, HEALTH_TIMEOUT) {
                Ok(md) => {
                    this.health_failures.store(0, Ordering::SeqCst);
                    Ok(md.brokers().len())
                }
                Err(e) => {
                    let failures = this.health_failures.fetch_add(1, Ordering::SeqCst) + 1;
                    if failures >= RECOVERY_THRESHOLD
                        && let Some(brokers) = this.try_recover()
                    {
                        return Ok(brokers);
                    }
                    Err(e)
                }
            }
        }).await;
        match res {
            Ok(Ok(brokers)) => ClusterHealth { status: HealthStatus::Healthy, broker_count: Some(brokers), error: None },
            Ok(Err(e)) => ClusterHealth { status: HealthStatus::Unreachable, broker_count: None, error: Some(e.to_string()) },
            Err(e) => ClusterHealth { status: HealthStatus::Unreachable, broker_count: None, error: Some(e.to_string()) },
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
        let healed = (|| {
            let cc = build_client_config(&self.config);
            let consumer: BaseConsumer = cc.create().ok()?;
            let brokers = consumer.fetch_metadata(None, HEALTH_TIMEOUT).ok()?.brokers().len();
            let admin: AdminClient<DefaultClientContext> = cc.create().ok()?;
            self.swap_clients(consumer, admin);
            self.health_failures.store(0, Ordering::SeqCst);
            tracing::warn!(cluster = %self.name, "stale kafka connection replaced with a fresh one");
            Some(brokers)
        })();
        self.probe_in_flight.store(false, Ordering::SeqCst);
        healed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, SaslConfig, SaslMechanism};

    fn base(name: &str) -> ClusterConfig {
        ClusterConfig { name: name.into(), bootstrap: "b:9092".into(), sasl: None, schema_registry: None }
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
}
