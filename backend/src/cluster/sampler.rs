use super::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use rdkafka::consumer::Consumer;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

/// Demand-driven throughput sampling (owner design 2026-08-19). Nothing runs
/// in the background: `GET /throughput` takes a sample for the topic it was
/// asked about, at most once per sampling interval, so an unwatched cluster
/// costs nothing at all.
///
/// Two timers govern it. The **sampling interval** (the caller's, from
/// config) decouples sampling from request rate — several tabs polling one
/// topic still produce one sample per interval. The **horizon** below bounds
/// how old a baseline may be before it stops describing "now".
const HORIZON_MS: i64 = 15 * 60_000;

/// Slack on top of the sampling interval before a window counts as a hole:
/// one slow poll must not read as "the viewer was away", and a zero interval
/// (sample on every request) must still produce continuous stretches.
const JITTER_MS: i64 = 5_000;

#[derive(Debug, Clone, Copy)]
struct TopicSample {
    ts_ms: i64,
    total_msgs: i64,
}

#[derive(Debug, Serialize)]
pub struct RatePoint {
    pub ts_ms: i64,
    pub msgs_per_sec: f64,
    /// How long this rate was measured over: one sampling interval while a
    /// viewer is watching, longer for the first sample after they come back.
    pub window_ms: i64,
    /// False when the window spans more than one interval — the stretch was
    /// never observed, so a chart must break its line here rather than draw
    /// a rate we did not see.
    pub continuous: bool,
    pub bytes_per_sec: Option<f64>, // always None (no DescribeLogDirs in librdkafka)
}

pub struct SamplerStore {
    inner: RwLock<HashMap<String, VecDeque<TopicSample>>>,
}

impl Default for SamplerStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SamplerStore {
    pub fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    /// Milliseconds since this topic's newest sample, or `None` if it has
    /// never been sampled — the caller samples when this exceeds its interval.
    pub fn age(&self, topic: &str, now_ms: i64) -> Option<i64> {
        self.inner.read().unwrap().get(topic).and_then(|b| b.back().map(|s| now_ms - s.ts_ms))
    }

    pub fn record(&self, topic: &str, ts_ms: i64, total_msgs: i64) {
        let mut map = self.inner.write().unwrap();
        let buf = map.entry(topic.to_string()).or_default();
        // Anything beyond the horizon can neither be drawn nor serve as a
        // baseline; a shrinking total means the log itself moved backwards
        // (recreated, trimmed, reset), which no rate can describe.
        buf.retain(|s| ts_ms - s.ts_ms <= HORIZON_MS);
        if buf.back().is_some_and(|last| total_msgs < last.total_msgs) {
            buf.clear();
        }
        buf.push_back(TopicSample { ts_ms, total_msgs });
    }

    pub fn rate_points(&self, topic: &str, interval_ms: i64) -> Vec<RatePoint> {
        let map = self.inner.read().unwrap();
        let Some(buf) = map.get(topic) else { return Vec::new() };
        buf.iter()
            .zip(buf.iter().skip(1))
            .filter_map(|(a, b)| {
                let window_ms = b.ts_ms - a.ts_ms;
                if window_ms <= 0 {
                    return None;
                }
                let delta = b.total_msgs - a.total_msgs;
                Some(RatePoint {
                    ts_ms: b.ts_ms,
                    msgs_per_sec: delta as f64 / (window_ms as f64 / 1000.0),
                    window_ms,
                    continuous: window_ms <= interval_ms * 2 + JITTER_MS,
                    bytes_per_sec: None,
                })
            })
            .collect()
    }

    pub fn as_of(&self, topic: &str) -> Option<i64> {
        self.inner.read().unwrap().get(topic).and_then(|b| b.back().map(|s| s.ts_ms))
    }
}

/// One on-demand sample: the summed high watermark of every partition of
/// `topic`. All-or-nothing — a single failed partition fails the sample
/// rather than producing a total that silently omits data.
pub fn sample_topic_blocking(handle: &ClusterHandle, topic: &str) -> Result<i64, ApiError> {
    let md = handle.consumer()
        .fetch_metadata(Some(topic), ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
    let t = md.topics().iter()
        .find(|t| t.name() == topic && !t.partitions().is_empty())
        .ok_or_else(|| ApiError::topic_not_found(&handle.name, topic))?;
    let mut total = 0i64;
    for p in t.partitions() {
        let (_, hi) = handle.consumer()
            .fetch_watermarks(topic, p.id(), ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "fetch watermarks", &e))?;
        total += hi;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owner design 2026-08-19: sampling is demand-driven, so a topic's
    /// samples can be minutes apart. A rate carries the window it was
    /// measured over, and a window wider than the sampling interval is NOT
    /// continuous — the chart must not draw a line across time nobody
    /// measured.
    #[test]
    fn rate_is_delta_msgs_over_delta_seconds_and_carries_its_window() {
        let s = SamplerStore::new();
        s.record("t", 1_000, 0);
        s.record("t", 11_000, 50);
        let p = &s.rate_points("t", 10_000)[0];
        assert!((p.msgs_per_sec - 5.0).abs() < f64::EPSILON);
        assert_eq!(p.ts_ms, 11_000);
        assert_eq!(p.window_ms, 10_000);
        assert!(p.continuous, "consecutive samples one interval apart are continuous");
    }

    #[test]
    fn a_gap_wider_than_the_interval_yields_a_rate_that_is_not_continuous() {
        let s = SamplerStore::new();
        s.record("t", 0, 0);
        // viewer left and came back three minutes later
        s.record("t", 180_000, 360);
        let p = &s.rate_points("t", 10_000)[0];
        assert!((p.msgs_per_sec - 2.0).abs() < f64::EPSILON, "average over the gap is real data");
        assert_eq!(p.window_ms, 180_000);
        assert!(!p.continuous, "the chart must break the line across an unmeasured stretch");
    }

    /// A baseline older than the horizon cannot describe "now": the old
    /// samples are dropped and the new one starts fresh instead of
    /// reporting an average over an hour as a current rate.
    #[test]
    fn a_sample_past_the_horizon_starts_a_fresh_baseline() {
        let s = SamplerStore::new();
        s.record("t", 0, 0);
        s.record("t", HORIZON_MS + 1, 1_000_000);
        assert!(s.rate_points("t", 10_000).is_empty(), "no rate across a discarded baseline");
        assert_eq!(s.as_of("t"), Some(HORIZON_MS + 1));
    }

    /// Pruning is relative to the newest sample: everything strictly older
    /// than the horizon goes, everything inside it stays.
    #[test]
    fn samples_older_than_the_horizon_are_pruned() {
        let s = SamplerStore::new();
        s.record("t", 0, 0);                    // dropped below: 906s old
        s.record("t", 10_000, 10);              // kept: 896s old, inside 900s
        s.record("t", HORIZON_MS - 5_000, 100);
        s.record("t", HORIZON_MS + 6_000, 200);
        let points = s.rate_points("t", 10_000);
        assert_eq!(points.len(), 2, "three in-horizon samples make two rate points");
        assert_eq!(points[0].ts_ms, HORIZON_MS - 5_000, "the sample past the horizon is gone");
    }

    /// Owner ruling 2026-08-19: a shrinking total means the topic was
    /// recreated, trimmed by retention, or its offsets reset — a
    /// discontinuity, not a rate of zero. Reporting 0 msg/s would be a
    /// confident lie; the baseline breaks instead.
    #[test]
    fn a_shrinking_total_breaks_the_baseline_instead_of_reporting_zero() {
        let s = SamplerStore::new();
        s.record("t", 0, 100);
        s.record("t", 10_000, 40);
        assert!(s.rate_points("t", 10_000).is_empty(), "no rate is honest; 0 msg/s is not");
        assert_eq!(s.as_of("t"), Some(10_000));
    }

    #[test]
    fn age_reports_how_stale_the_newest_sample_is_and_drives_sampling() {
        let s = SamplerStore::new();
        assert_eq!(s.age("t", 5_000), None, "never sampled");
        s.record("t", 1_000, 10);
        assert_eq!(s.age("t", 5_000), Some(4_000));
    }

    #[test]
    fn unknown_topic_yields_empty() {
        let s = SamplerStore::new();
        assert!(s.rate_points("nope", 10_000).is_empty());
        assert_eq!(s.as_of("nope"), None);
    }
}
