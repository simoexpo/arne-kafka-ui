//! One expiring value per key, per cluster. The backend answers from here and
//! reaches the broker only when it has nothing fresh — so broker load follows
//! our refresh policy, never the number of people using Arne (owner ruling
//! 2026-08-20).
//!
//! The cache stores WHEN each value was sampled and nothing more: how long a
//! value stays usable is the caller's policy, because it differs per endpoint
//! (a topic's partitions and a consumer group's lag age differently, and lag
//! itself has two tiers). Values also carry their own `as_of` on the wire, so
//! a shared answer states its age instead of pretending to be new.
//!
//! Not for time series: `SamplerStore` keeps a sequence per topic and prunes
//! samples *inside* it, which is a different shape and stays separate.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;

#[derive(Clone)]
pub struct Stamped<V> {
    pub value: V,
    pub sampled_at: i64,
}

pub struct KeyedCache<K, V> {
    inner: RwLock<HashMap<K, Stamped<V>>>,
    /// Entries nobody has looked at for this long are dropped even if their
    /// key still exists on the cluster: browsing many topics must not grow
    /// memory without bound.
    horizon_ms: i64,
}

impl<K: Eq + Hash + Clone, V: Clone> KeyedCache<K, V> {
    pub fn new(horizon_ms: i64) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            horizon_ms,
        }
    }

    /// When this key was last sampled — a freshness probe that never clones
    /// the value.
    pub fn sampled_at(&self, key: &K) -> Option<i64> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|e| e.sampled_at)
    }

    /// Asks a question OF the stored value without copying it. Cloning a
    /// group's whole row set just to test a boolean is the kind of waste that
    /// scales with cluster size.
    pub fn with<R>(&self, key: &K, f: impl FnOnce(&V) -> R) -> Option<R> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|e| f(&e.value))
    }

    pub fn get(&self, key: &K) -> Option<Stamped<V>> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
    }

    /// The value only if it is younger than `ttl_ms`.
    pub fn fresh(&self, key: &K, ttl_ms: i64, now: i64) -> Option<V> {
        self.get(key)
            .filter(|e| now - e.sampled_at < ttl_ms)
            .map(|e| e.value)
    }

    pub fn insert(&self, key: K, value: V, now: i64) {
        self.insert_many([(key, value)], now);
    }

    /// One lock and one horizon sweep for the whole batch: inserting a
    /// thousand partitions one at a time would sweep a map that holds every
    /// recently-viewed partition, a thousand times.
    pub fn insert_many(&self, entries: impl IntoIterator<Item = (K, V)>, now: i64) {
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, e| now - e.sampled_at < self.horizon_ms);
        for (key, value) in entries {
            map.insert(
                key,
                Stamped {
                    value,
                    sampled_at: now,
                },
            );
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip_per_key() {
        let cache: KeyedCache<String, i32> = KeyedCache::new(600_000);
        cache.insert("a".into(), 1, 1_000);
        cache.insert("b".into(), 2, 1_000);
        assert_eq!(cache.get(&"a".to_string()).unwrap().value, 1);
        assert_eq!(cache.get(&"b".to_string()).unwrap().value, 2);
        assert!(cache.get(&"c".to_string()).is_none());
    }

    /// The hazard that makes a keyed cache necessary: one slot would serve
    /// topic B's data under topic A's name.
    #[test]
    fn one_key_never_answers_for_another() {
        let cache: KeyedCache<String, &str> = KeyedCache::new(600_000);
        cache.insert("orders".into(), "orders-data", 1_000);
        cache.insert("users".into(), "users-data", 1_000);
        assert_eq!(
            cache.fresh(&"orders".to_string(), 10_000, 1_500),
            Some("orders-data")
        );
        assert_eq!(
            cache.fresh(&"users".to_string(), 10_000, 1_500),
            Some("users-data")
        );
    }

    #[test]
    fn freshness_is_the_callers_policy() {
        let cache: KeyedCache<&str, i32> = KeyedCache::new(600_000);
        cache.insert("k", 7, 0);
        assert_eq!(cache.fresh(&"k", 5_000, 4_999), Some(7));
        assert_eq!(
            cache.fresh(&"k", 5_000, 5_000),
            None,
            "expired under a 5s policy"
        );
        assert_eq!(
            cache.fresh(&"k", 60_000, 5_000),
            Some(7),
            "still fine under a 60s one"
        );
        assert_eq!(
            cache.sampled_at(&"k"),
            Some(0),
            "the probe never needs the value"
        );
    }

    #[test]
    fn writes_drop_entries_past_the_horizon() {
        let cache: KeyedCache<&str, i32> = KeyedCache::new(10_000);
        cache.insert("old", 1, 0);
        cache.insert("new", 2, 10_001);
        assert_eq!(cache.len(), 1, "the stale entry went with the write");
        assert!(cache.get(&"old").is_none());
    }

    /// A keyed hit shares its payload too, when the caller parameterises the
    /// cache with an `Arc` — which the big values do and the eight-byte
    /// watermarks deliberately do not.
    #[test]
    fn a_keyed_hit_shares_the_payload_when_the_value_is_shared() {
        use std::sync::Arc;
        let cache: KeyedCache<&str, Arc<Vec<u32>>> = KeyedCache::new(600_000);
        cache.insert("k", Arc::new(vec![1, 2, 3]), 0);
        let a = cache.get(&"k").unwrap().value;
        let b = cache.get(&"k").unwrap().value;
        assert!(Arc::ptr_eq(&a, &b), "two reads, one allocation");
    }

    /// Asking a question of the value must not copy it: cloning a group's
    /// whole row set to test a boolean is waste that scales with the cluster.
    #[test]
    fn with_answers_without_copying_the_value() {
        let cache: KeyedCache<&str, Vec<i32>> = KeyedCache::new(600_000);
        cache.insert("k", vec![1, 2, 3], 0);
        assert_eq!(cache.with(&"k", |v| v.len()), Some(3));
        assert_eq!(cache.with(&"missing", |v| v.len()), None);
    }
}
