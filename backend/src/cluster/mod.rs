pub mod admin;
pub mod registry;

use crate::config::{ClusterConfig, SaslMechanism};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::ClientConfig;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

pub const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

pub fn build_client_config(cfg: &ClusterConfig) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", &cfg.bootstrap);
    cc.set("allow.auto.create.topics", "false");
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
    consumer: Arc<BaseConsumer>,
    admin: AdminClient<DefaultClientContext>,
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
        Ok(Self { name: config.name.clone(), config, consumer: Arc::new(consumer), admin })
    }

    pub fn consumer(&self) -> &BaseConsumer { &self.consumer }
    pub fn admin(&self) -> &AdminClient<DefaultClientContext> { &self.admin }

    pub async fn health(self: &Arc<Self>) -> ClusterHealth {
        let this = self.clone();
        let res = tokio::task::spawn_blocking(move || {
            this.consumer.fetch_metadata(None, HEALTH_TIMEOUT).map(|md| md.brokers().len())
        }).await;
        match res {
            Ok(Ok(brokers)) => ClusterHealth { status: HealthStatus::Healthy, broker_count: Some(brokers), error: None },
            Ok(Err(e)) => ClusterHealth { status: HealthStatus::Unreachable, broker_count: None, error: Some(e.to_string()) },
            Err(e) => ClusterHealth { status: HealthStatus::Unreachable, broker_count: None, error: Some(e.to_string()) },
        }
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
}
