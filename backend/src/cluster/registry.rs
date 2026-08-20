use super::ClusterHandle;
use crate::config::ClusterConfig;
use crate::error::ApiError;
use rdkafka::error::KafkaResult;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ClusterRegistry {
    map: HashMap<String, Arc<ClusterHandle>>,
    order: Vec<String>,
}

impl ClusterRegistry {
    pub fn from_config(clusters: Vec<ClusterConfig>) -> KafkaResult<Self> {
        let mut map = HashMap::new();
        let mut order = Vec::new();
        for c in clusters {
            order.push(c.name.clone());
            map.insert(c.name.clone(), Arc::new(ClusterHandle::connect(c)?));
        }
        Ok(Self { map, order })
    }

    pub fn get(&self, name: &str) -> Result<Arc<ClusterHandle>, ApiError> {
        self.map.get(name).cloned().ok_or_else(|| ApiError::cluster_not_found(name))
    }

    pub fn all(&self) -> Vec<Arc<ClusterHandle>> {
        self.order.iter().filter_map(|n| self.map.get(n).cloned()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    fn cfg(name: &str) -> ClusterConfig {
        ClusterConfig { name: name.into(), bootstrap: "localhost:19092".into(), sasl: None, schema_registry: None, broker_call_stats_ms: 0 }
    }

    #[test]
    fn get_returns_handle_or_404() {
        let reg = ClusterRegistry::from_config(vec![cfg("a"), cfg("b")]).unwrap();
        assert_eq!(reg.get("a").unwrap().name, "a");
        assert_eq!(reg.all().len(), 2);
        let err = reg.get("nope").unwrap_err();
        assert_eq!(err.code, "cluster_not_found");
    }
}
