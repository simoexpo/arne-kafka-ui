//! What Arne actually asked the brokers, counted by librdkafka itself.
//!
//! librdkafka already tallies every request it sends, per broker and per API,
//! and hands the tally to a callback on a timer. Nothing here counts anything:
//! it keeps the latest tally per client so a view — or a test — can state the
//! cost of a page instead of inferring it from reading call paths.
//!
//! Per broker, not just per API, because that is the only place a fan-out is
//! visible: one call from our side can be one request to every broker in the
//! cluster, and a total hides that completely.
//!
//! Off unless a stats interval is configured. With no interval librdkafka
//! never emits a tally, so this costs exactly nothing.
//!
//! Tallies QUEUE inside librdkafka until something drains them, and only a
//! read of the report drains them (deliberately: nothing polls on a timer, so
//! an idle Arne does nothing). Leaving the interval set with nobody ever
//! reading therefore accumulates blobs — so set it while diagnosing, not for
//! ever. It is off by default for that reason.
//!
//! This is the whole picture for a cluster's resident client, because there is
//! only one: every admin call goes out on it through `ffi::AdminCall` rather
//! than through a second `AdminClient`, whose tally rust-rdkafka cannot
//! deliver (it gives that client no event mask and no stats callback, so
//! librdkafka discards its stats before anything can read them).
//!
//! What it does NOT count: the short-lived consumers `message::fetch`/`tail`
//! build per request and destroy again. Their traffic is bounded by one
//! request each and dies with them, so there is no cumulative tally to keep.

use rdkafka::Statistics;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Which client a tally came from. Kept per role and summed on read, so a
/// second resident client would be accounted for without changing a reader.
pub const CONSUMER: &str = "consumer";
#[cfg(test)]
pub const ADMIN: &str = "admin";

#[derive(Debug, Clone, Default)]
struct Tally {
    sampled_at: i64,
    /// broker node name -> API name -> requests sent since that client began.
    per_broker: BTreeMap<String, BTreeMap<String, u64>>,
}

#[derive(Default)]
pub struct CallStats {
    tallies: Mutex<BTreeMap<&'static str, Tally>>,
    /// How many tallies have been recorded. librdkafka stamps its own with
    /// whole seconds, which is far too coarse to tell one tally from the next
    /// at a sub-second interval — a reader that needs "a tally taken after
    /// this moment" counts them instead of comparing stamps.
    recorded: AtomicU64,
    /// Bumped whenever the resident clients are replaced. The counts are
    /// cumulative PER CLIENT INSTANCE, so they restart from zero on a swap —
    /// a difference taken across a swap would be nonsense, and a reader can
    /// see that here rather than reading a negative delta as a fall in load.
    generation: AtomicU64,
}

impl CallStats {
    pub fn record(&self, role: &'static str, stats: &Statistics) {
        let per_broker = stats
            .brokers
            .values()
            // A logical broker (bootstrap entry, coordinator alias) carries no
            // node id and sends nothing of its own worth attributing.
            .filter(|b| b.nodeid >= 0)
            .map(|b| {
                let calls = b.req.iter()
                    .filter(|(_, sent)| **sent > 0)
                    .map(|(api, sent)| (api.clone(), *sent as u64))
                    .collect();
                (b.nodename.clone(), calls)
            })
            .collect();
        let tally = Tally { sampled_at: stats.time * 1_000, per_broker };
        self.tallies.lock().expect("call stats lock poisoned").insert(role, tally);
        self.recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// How many tallies have been taken. A caller draining the client's queue
    /// watches this to know whether a poll delivered anything.
    pub fn recorded(&self) -> u64 {
        self.recorded.load(Ordering::Relaxed)
    }

    pub fn client_replaced(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.tallies.lock().expect("call stats lock poisoned").clear();
    }

    /// The latest tally from every client, summed. `sampled_at` is the OLDEST
    /// of the tallies it merges: the answer is only as fresh as its stalest
    /// part, and a count presented under a newer stamp than it earned would
    /// be a lie about when Arne made those calls.
    pub fn report(&self) -> CallReport {
        let tallies = self.tallies.lock().expect("call stats lock poisoned");
        let mut brokers: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        let mut sampled_at: Option<i64> = None;
        for tally in tallies.values() {
            sampled_at = Some(sampled_at.map_or(tally.sampled_at, |o: i64| o.min(tally.sampled_at)));
            for (broker, calls) in &tally.per_broker {
                let entry = brokers.entry(broker.clone()).or_default();
                for (api, sent) in calls {
                    *entry.entry(api.clone()).or_insert(0) += sent;
                }
            }
        }
        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        for calls in brokers.values() {
            for (api, sent) in calls {
                *totals.entry(api.clone()).or_insert(0) += sent;
            }
        }
        CallReport {
            sampled_at,
            recorded: self.recorded.load(Ordering::Relaxed),
            generation: self.generation.load(Ordering::Relaxed),
            totals,
            brokers: brokers.into_iter()
                .map(|(broker, calls)| BrokerCalls { broker, calls })
                .collect(),
        }
    }
}

/// The client context that receives librdkafka's tallies. One per resident
/// client, sharing the cluster's store, so the consumer's and the admin's
/// requests are both accounted for under their own role.
#[derive(Clone)]
pub struct StatsContext {
    stats: std::sync::Arc<CallStats>,
    role: &'static str,
}

impl StatsContext {
    pub fn new(stats: std::sync::Arc<CallStats>, role: &'static str) -> Self {
        Self { stats, role }
    }
}

impl rdkafka::ClientContext for StatsContext {
    // Only reached when `statistics.interval.ms` is set, so this whole
    // mechanism costs nothing when the interval is not configured.
    fn stats(&self, statistics: Statistics) {
        self.stats.record(self.role, &statistics);
    }
}

impl rdkafka::consumer::ConsumerContext for StatsContext {}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BrokerCalls {
    pub broker: String,
    pub calls: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CallReport {
    /// `null` when no tally has arrived yet — the stats interval is not
    /// configured, or the first one has not elapsed.
    pub sampled_at: Option<i64>,
    /// How many tallies this cluster has recorded. A caller wanting a tally
    /// taken after some moment waits for this to advance.
    pub recorded: u64,
    pub generation: u64,
    pub totals: BTreeMap<String, u64>,
    pub brokers: Vec<BrokerCalls>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(name: &str, nodeid: i32, req: &[(&str, i64)]) -> rdkafka::statistics::Broker {
        let mut b = rdkafka::statistics::Broker {
            name: name.to_string(),
            nodeid,
            nodename: name.to_string(),
            ..Default::default()
        };
        b.req = req.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        b
    }

    fn stats_at(time: i64, brokers: Vec<rdkafka::statistics::Broker>) -> Statistics {
        Statistics {
            time,
            brokers: brokers.into_iter().map(|b| (b.name.clone(), b)).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_fan_out_is_visible_per_broker_not_just_in_the_total() {
        let stats = CallStats::default();
        stats.record(
            CONSUMER,
            &stats_of(&[("b1", 1, &[("ListGroups", 1)]), ("b2", 2, &[("ListGroups", 1)]), ("b3", 3, &[("ListGroups", 1)])]),
        );
        let report = stats.report();
        assert_eq!(report.totals["ListGroups"], 3, "one call from us, three requests");
        assert_eq!(report.brokers.len(), 3, "and the fan-out is named broker by broker");
    }

    /// (node name, node id, that broker's request counts)
    type BrokerSpec<'a> = (&'a str, i32, &'a [(&'a str, i64)]);

    fn stats_of(brokers: &[BrokerSpec<'_>]) -> Statistics {
        stats_at(1_000, brokers.iter().map(|(n, id, req)| broker(n, *id, req)).collect())
    }

    /// Both resident clients count only their own requests, so a page's cost
    /// is the sum — and the answer is only as fresh as the stalest half.
    #[test]
    fn the_clients_are_summed_and_stamped_with_the_oldest_tally() {
        let stats = CallStats::default();
        stats.record(CONSUMER, &stats_at(9, vec![broker("b1", 1, &[("Metadata", 4)])]));
        stats.record(ADMIN, &stats_at(5, vec![broker("b1", 1, &[("Metadata", 1), ("DescribeGroups", 2)])]));
        let report = stats.report();
        assert_eq!(report.totals["Metadata"], 5);
        assert_eq!(report.totals["DescribeGroups"], 2);
        assert_eq!(report.sampled_at, Some(5_000), "as old as the older half");
    }

    /// A bootstrap or coordinator alias has no node id and no requests of its
    /// own to attribute; counting it would double what a real broker was sent.
    #[test]
    fn logical_brokers_are_not_counted() {
        let stats = CallStats::default();
        stats.record(CONSUMER, &stats_of(&[("bootstrap", -1, &[("Metadata", 7)]), ("b1", 1, &[("Metadata", 2)])]));
        assert_eq!(stats.report().totals["Metadata"], 2);
    }

    /// Counts restart from zero when a client is replaced. A reader differencing
    /// across that boundary must be able to tell, instead of reading the reset
    /// as a drop in load.
    #[test]
    fn replacing_a_client_bumps_the_generation_and_drops_the_counts() {
        let stats = CallStats::default();
        stats.record(CONSUMER, &stats_of(&[("b1", 1, &[("Metadata", 3)])]));
        assert_eq!(stats.report().generation, 0);
        stats.client_replaced();
        let report = stats.report();
        assert_eq!(report.generation, 1);
        assert_eq!(report.recorded, 1, "the count of tallies seen is not itself reset");
        assert!(report.totals.is_empty(), "the old client's counts are gone with it");
        assert_eq!(report.sampled_at, None);
    }
}
