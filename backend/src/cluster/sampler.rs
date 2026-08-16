use super::{ClusterHandle, ADMIN_TIMEOUT};
use rdkafka::consumer::Consumer;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct TopicSample {
    pub ts_ms: i64,
    pub total_msgs: i64,
}

#[derive(Debug, Serialize)]
pub struct RatePoint {
    pub ts_ms: i64,
    pub msgs_per_sec: f64,
    pub bytes_per_sec: Option<f64>, // always None in v1 (no DescribeLogDirs in librdkafka)
}

pub struct SamplerStore {
    window_len: usize,
    inner: RwLock<HashMap<String, VecDeque<TopicSample>>>,
}

impl SamplerStore {
    pub fn new(window_len: usize) -> Self {
        Self { window_len, inner: RwLock::new(HashMap::new()) }
    }

    pub fn record(&self, topic: &str, sample: TopicSample) {
        let mut map = self.inner.write().unwrap();
        let buf = map.entry(topic.to_string()).or_default();
        buf.push_back(sample);
        while buf.len() > self.window_len {
            buf.pop_front();
        }
    }

    pub fn rate_points(&self, topic: &str) -> Vec<RatePoint> {
        let map = self.inner.read().unwrap();
        let Some(buf) = map.get(topic) else { return Vec::new() };
        buf.iter().zip(buf.iter().skip(1)).map(|(a, b)| {
            let dt = (b.ts_ms - a.ts_ms) as f64 / 1000.0;
            let dm = (b.total_msgs - a.total_msgs) as f64;
            RatePoint {
                ts_ms: b.ts_ms,
                msgs_per_sec: if dt > 0.0 { (dm / dt).max(0.0) } else { 0.0 },
                bytes_per_sec: None,
            }
        }).collect()
    }

    pub fn as_of(&self, topic: &str) -> Option<i64> {
        self.inner.read().unwrap().get(topic).and_then(|b| b.back().map(|s| s.ts_ms))
    }
}

pub fn spawn_sampler(handle: Arc<ClusterHandle>, interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let h = handle.clone();
            let _ = tokio::task::spawn_blocking(move || sample_once(&h, ADMIN_TIMEOUT)).await;
            tokio::time::sleep(interval).await;
        }
    })
}

/// For EVERY topic, internal (`__`-prefixed) included, sums its partitions'
/// high watermarks via `fetch_hi`, tolerating a per-topic failure the same
/// way `admin::assemble_topic_estimate` does: one failed partition drops
/// that topic's sample entirely rather than reporting a partial/misleading
/// total (`complete` is all-or-nothing).
///
/// B2 (queue-review): an earlier revision skipped `__`-prefixed topics here
/// too, mirroring `admin::list_topics`'s own skip — but that skip only
/// suppresses a *count* there; here it silently zeroed out
/// `sampler.rate_points`, the sole source for `GET /throughput`, leaving an
/// internal topic's Throughput/Sparkline panel permanently and unexplainedly
/// empty. Chart correctness beats the tick tax, so the skip was reverted.
/// The tax itself (every internal topic's watermark fetch runs on every
/// sampler tick, even for topics no one has open) is a real cost worth
/// revisiting with a better shape — e.g. lazy/on-demand sampling keyed off
/// which topic's detail page is actually open — tracked in
/// `docs/superpowers/plans/2026-08-15-sliding-window-followups.md`, not
/// attempted here.
fn topic_totals<'a>(
    topics: impl IntoIterator<Item = (&'a str, &'a [i32])>,
    mut fetch_hi: impl FnMut(&str, i32) -> Result<i64, ()>,
) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for (name, partitions) in topics {
        let mut total = 0i64;
        let mut complete = true;
        for &p in partitions {
            match fetch_hi(name, p) {
                Ok(hi) => total += hi,
                Err(()) => { complete = false; break; }
            }
        }
        if complete {
            out.push((name.to_string(), total));
        }
    }
    out
}

fn sample_once(handle: &ClusterHandle, timeout: Duration) {
    let now = crate::util::now_ms();
    let md = match handle.consumer().fetch_metadata(None, timeout) {
        Ok(md) => {
            handle.reset_shared_failures();
            md
        }
        Err(_) => {
            // headless heal: without this, a wedged client starves the
            // sampler forever until a browser opens /api/clusters
            handle.note_shared_failure_and_maybe_recover();
            return;
        }
    };
    // Owned (name, partition_ids) pairs — `topic_totals` borrows from this,
    // so the partition-id vectors need somewhere to live past the `map`
    // that builds them; the actual skip/tolerate-failure policy is entirely
    // in `topic_totals` itself.
    let topics: Vec<(String, Vec<i32>)> = md.topics().iter()
        .map(|t| (t.name().to_string(), t.partitions().iter().map(|p| p.id()).collect()))
        .collect();
    let topics_ref = topics.iter().map(|(name, ids)| (name.as_str(), ids.as_slice()));
    let totals = topic_totals(topics_ref, |name, partition| {
        handle.consumer().fetch_watermarks(name, partition, timeout).map(|(_, hi)| hi).map_err(|_| ())
    });
    for (name, total) in totals {
        handle.sampler.record(&name, TopicSample { ts_ms: now, total_msgs: total });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// B2 revert: an internal (`__`-prefixed) topic must be sampled exactly
    /// like any other topic. `admin::list_topics` skips them for its
    /// inventory *count* — safe there, since nothing downstream renders a
    /// chart off it — but `sampler.rate_points` is the sole source for
    /// `GET /throughput`, which `ConsumersTab` renders as a live
    /// Throughput/Sparkline panel for whatever topic the user opened,
    /// internal or not. Skipping the fetch here left that chart
    /// permanently, silently empty for internal topics — the exact
    /// "blank without a reason" pattern the sibling I5 tooltip work
    /// existed to avoid. Proven by pointer-free means: the fetch closure
    /// IS invoked for the internal topic's every partition (the call
    /// counter covers both topics' partitions), not merely that its
    /// result isn't discarded.
    #[test]
    fn internal_topics_are_sampled_like_any_other_topic() {
        let calls = RefCell::new(0);
        let topics = [("__consumer_offsets", &[0, 1, 2, 3][..]), ("orders", &[0][..])];
        let totals = topic_totals(topics, |_name, _partition| {
            *calls.borrow_mut() += 1;
            Ok(10)
        });
        assert_eq!(totals, vec![("__consumer_offsets".to_string(), 40), ("orders".to_string(), 10)]);
        assert_eq!(*calls.borrow(), 5, "every partition of every topic, internal included, must be fetched");
    }

    #[test]
    fn a_topic_with_a_failed_partition_yields_no_sample() {
        let totals = topic_totals([("orders", &[0, 1][..])], |_name, partition| {
            if partition == 1 { Err(()) } else { Ok(5) }
        });
        assert!(totals.is_empty(), "an incomplete topic must not report a partial/misleading total");
    }

    #[test]
    fn multiple_real_topics_are_all_sampled() {
        let totals = topic_totals(
            [("a", &[0][..]), ("b", &[0, 1][..])],
            |_name, _partition| Ok(7),
        );
        assert_eq!(totals, vec![("a".to_string(), 7), ("b".to_string(), 14)]);
    }

    #[test]
    fn rate_is_delta_msgs_over_delta_seconds() {
        let s = SamplerStore::new(10);
        s.record("t", TopicSample { ts_ms: 1_000, total_msgs: 0 });
        s.record("t", TopicSample { ts_ms: 11_000, total_msgs: 50 });
        s.record("t", TopicSample { ts_ms: 21_000, total_msgs: 50 });
        let points = s.rate_points("t");
        assert_eq!(points.len(), 2);
        assert!((points[0].msgs_per_sec - 5.0).abs() < f64::EPSILON);
        assert_eq!(points[0].ts_ms, 11_000);
        assert!((points[1].msgs_per_sec - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn window_evicts_oldest() {
        let s = SamplerStore::new(3);
        for i in 0..5 {
            s.record("t", TopicSample { ts_ms: i * 1000, total_msgs: i * 10 });
        }
        assert_eq!(s.rate_points("t").len(), 2); // 3 samples kept → 2 rate points
        assert_eq!(s.as_of("t"), Some(4000));
    }

    #[test]
    fn shrinking_offsets_clamp_to_zero() {
        // retention/truncation can shrink totals; a negative rate must never be shown
        let s = SamplerStore::new(10);
        s.record("t", TopicSample { ts_ms: 0, total_msgs: 100 });
        s.record("t", TopicSample { ts_ms: 10_000, total_msgs: 40 });
        assert_eq!(s.rate_points("t")[0].msgs_per_sec, 0.0);
    }

    #[test]
    fn unknown_topic_yields_empty() {
        let s = SamplerStore::new(10);
        assert!(s.rate_points("nope").is_empty());
        assert_eq!(s.as_of("nope"), None);
    }

    #[test]
    fn failed_sample_feeds_the_recovery_counter() {
        use crate::config::ClusterConfig;
        use std::time::Duration;

        let cfg = ClusterConfig { name: "s".into(), bootstrap: "127.0.0.1:1".into(), sasl: None, schema_registry: None };
        let handle = ClusterHandle::connect(cfg).expect("lazy create");
        // short timeout keeps this fast; production passes ADMIN_TIMEOUT
        sample_once(&handle, Duration::from_millis(300));
        assert_eq!(handle.health_failures.load(std::sync::atomic::Ordering::SeqCst), 1);
        // second failed sample reaches the threshold and attempts a probe
        // (fails against the dead endpoint — honest, bounded by HEALTH_TIMEOUT)
        sample_once(&handle, Duration::from_millis(300));
        assert_eq!(handle.health_failures.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!handle.probe_in_flight.load(std::sync::atomic::Ordering::SeqCst));
    }
}
