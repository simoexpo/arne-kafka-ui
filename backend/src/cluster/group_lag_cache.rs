//! Reactive per-cluster cache behind `/topics/{t}/consumers` (owner design
//! 2026-08-19). No background task: refreshes happen only when a request
//! arrives, and only for entries whose age says they're due. Idle cluster →
//! zero broker calls.
//!
//! Tiering: a topic's own consumers — live members assigned to it, and
//! inspected groups that turned out to hold offsets on it — refresh at the
//! page's own cadence. Groups we must inspect only because they are
//! unfilterable (empty or undecodable) and that last showed NO offsets on
//! the topic refresh on the slow tier: they produce no rows, so the slower
//! tier delays nothing a viewer can see — only the discovery of the rare
//! member-less new committer (documented, at most `IDLE_TTL_MS` late).

use super::admin::PartitionLag;
use super::keyed_cache::KeyedCache;

/// Deliberately BELOW the consumers tab's 10s poll interval: an entry
/// refreshed on one poll must be due again on the next, or a poll would
/// serve a nearly-expired entry and the effective cadence would wobble
/// between 10s and 20s. Polls arriving within the window — a second tab, a
/// manual reload — are served from the entry instead of refetching it;
/// requests that are genuinely simultaneous each see it as due and both
/// refresh (there is no single-flight, and the TTL alone bounds the cost).
pub const LIVE_TTL_MS: i64 = 8_000;
pub const IDLE_TTL_MS: i64 = 60_000;
/// A group nobody has viewed for this long is dropped even if it still
/// exists on the cluster — the next view re-inspects it.
const EVICT_AGE_MS: i64 = 600_000;

/// How a group relates to the topic being viewed, derived from the
/// group-list's member assignment blobs (`assignment::assigned_topics`).
#[derive(Debug, PartialEq)]
pub enum Classification {
    /// A live member is assigned to the topic: a current consumer.
    AssignedToTopic,
    /// Empty (no members) or any member's assignment is undecodable —
    /// unfilterable, its committed offsets must be inspected (fail open).
    MustInspect,
    /// Live, every member decoded, none assigned to the topic: the group
    /// ended its relationship with this topic. Hidden here — its residual
    /// offsets stay visible on the group's own detail page until Kafka
    /// expires them (owner ruling 2026-08-19).
    MovedAway,
}

/// `member_topics` carries one entry per live member: `Some(topics)` when
/// its assignment blob decoded, `None` when it didn't (or was absent).
pub fn classify(member_topics: &[Option<Vec<String>>], topic: &str) -> Classification {
    if member_topics.is_empty() {
        return Classification::MustInspect;
    }
    let mut certain = true;
    for m in member_topics {
        match m {
            Some(topics) if topics.iter().any(|t| t == topic) => {
                return Classification::AssignedToTopic;
            }
            // A member assigned NOTHING is not evidence the group moved on:
            // mid-rebalance revocation and a member idle on a topic with
            // fewer partitions than members both look exactly like this.
            Some(topics) if topics.is_empty() => certain = false,
            Some(_) => {}
            None => certain = false,
        }
    }
    if certain {
        Classification::MovedAway
    } else {
        Classification::MustInspect
    }
}

/// Lag rows for one `(scope, group)`, where scope is a topic name or `"*"`
/// for the cluster-wide view the consumers list shows.
pub type LagKey = (String, String);

#[derive(Clone)]
pub struct CachedEntry {
    pub partitions: Vec<PartitionLag>,
    pub sampled_at: i64,
}

/// The two facts a due-check needs, read without cloning the rows.
#[derive(Clone, Copy)]
pub struct Freshness {
    pub sampled_at: i64,
    pub has_rows: bool,
}

/// An entry is due when absent, or older than its tier. Rows the viewer can
/// see (a current consumer, or an inspected group holding offsets on the
/// topic) live on the fast tier; inspected groups without offsets on the
/// topic live on the slow one.
pub fn needs_refresh(class: &Classification, entry: Option<Freshness>, now_ms: i64) -> bool {
    let Some(e) = entry else { return true };
    let ttl = match class {
        Classification::AssignedToTopic => LIVE_TTL_MS,
        Classification::MustInspect => {
            if e.has_rows { LIVE_TTL_MS } else { IDLE_TTL_MS }
        }
        // callers skip MovedAway groups before asking; this arm only
        // keeps the match total
        Classification::MovedAway => return false,
    };
    now_ms - e.sampled_at >= ttl
}

/// Lag storage: a `KeyedCache` with the tier policy above layered on top.
/// The generic cache holds "one expiring value per key"; which TTL applies to
/// which row is domain policy and stays here.
pub struct GroupLagCache {
    inner: KeyedCache<LagKey, Vec<PartitionLag>>,
}

impl Default for GroupLagCache {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupLagCache {
    pub fn new() -> Self {
        Self { inner: KeyedCache::new(EVICT_AGE_MS) }
    }

    pub fn freshness(&self, scope: &str, group: &str) -> Option<Freshness> {
        self.inner.get(&key(scope, group)).map(|e| Freshness {
            sampled_at: e.sampled_at,
            has_rows: !e.value.is_empty(),
        })
    }

    pub fn get(&self, scope: &str, group: &str) -> Option<CachedEntry> {
        self.inner
            .get(&key(scope, group))
            .map(|e| CachedEntry { partitions: e.value, sampled_at: e.sampled_at })
    }

    pub fn insert(&self, scope: &str, group: &str, entry: CachedEntry) {
        self.inner.insert(key(scope, group), entry.partitions, entry.sampled_at);
    }

    /// Drop `scope`'s entries whose group is gone from the caller's group
    /// list. `as_of` is that caller's snapshot time: an entry sampled at or
    /// after it belongs to a request with a newer view of the cluster, so it
    /// survives — otherwise two concurrent polls would delete each other's
    /// fresh work and re-fetch it.
    pub fn evict(&self, scope: &str, keep_groups: &dyn Fn(&str) -> bool, as_of: i64) {
        self.inner.retain(as_of, |(s, g), sampled_at| {
            s != scope || sampled_at >= as_of || keep_groups(g)
        });
    }
}

fn key(scope: &str, group: &str) -> LagKey {
    (scope.to_string(), group.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lag_row() -> PartitionLag {
        PartitionLag { topic: "t".into(), partition: 0, committed_offset: 1, end_offset: 2, lag: 1 }
    }

    fn fresh(sampled_at: i64, has_rows: bool) -> Option<Freshness> {
        Some(Freshness { sampled_at, has_rows })
    }

    #[test]
    fn empty_group_must_be_inspected() {
        assert_eq!(classify(&[], "t"), Classification::MustInspect);
    }

    #[test]
    fn any_undecodable_member_forces_inspection() {
        // one member decoded onto another topic, one undecodable: the group
        // could be consuming anything — fail open into the check
        assert_eq!(
            classify(&[Some(vec!["other".into()]), None], "t"),
            Classification::MustInspect
        );
    }

    #[test]
    fn a_member_assigned_to_the_topic_wins_even_with_undecodable_siblings() {
        assert_eq!(
            classify(&[None, Some(vec!["t".into(), "other".into()])], "t"),
            Classification::AssignedToTopic
        );
    }

    #[test]
    fn fully_decoded_elsewhere_is_moved_away() {
        assert_eq!(
            classify(&[Some(vec!["a".into()]), Some(vec!["b".into()])], "t"),
            Classification::MovedAway
        );
    }

    /// A well-formed assignment holding NO topics means "this member has
    /// nothing right now" — a rebalance revocation, or a member idle because
    /// the topic has fewer partitions than the group has members. Reading it
    /// as "moved away" would drop an actively-consuming group out of the tab
    /// mid-rebalance and evict its cached lag.
    #[test]
    fn a_member_assigned_nothing_forces_inspection() {
        assert_eq!(classify(&[Some(vec![])], "t"), Classification::MustInspect);
        assert_eq!(
            classify(&[Some(vec![]), Some(vec!["other".into()])], "t"),
            Classification::MustInspect
        );
        // ...but a sibling actually holding the topic still wins outright
        assert_eq!(
            classify(&[Some(vec![]), Some(vec!["t".into()])], "t"),
            Classification::AssignedToTopic
        );
    }

    #[test]
    fn missing_entry_always_refreshes() {
        assert!(needs_refresh(&Classification::AssignedToTopic, None, 0));
        assert!(needs_refresh(&Classification::MustInspect, None, 0));
    }

    #[test]
    fn assigned_groups_refresh_on_the_fast_tier() {
        assert!(!needs_refresh(&Classification::AssignedToTopic, fresh(0, false), LIVE_TTL_MS - 1));
        assert!(needs_refresh(&Classification::AssignedToTopic, fresh(0, false), LIVE_TTL_MS));
    }

    /// The fast tier must expire strictly before the page's 10s poll, so
    /// every poll of a visible row actually refreshes it instead of serving
    /// an almost-due entry and doubling the effective cadence.
    #[test]
    fn fast_tier_expires_before_the_pages_poll_interval() {
        const { assert!(LIVE_TTL_MS < 10_000) };
        assert!(needs_refresh(&Classification::AssignedToTopic, fresh(0, true), 10_000));
        assert!(needs_refresh(&Classification::MustInspect, fresh(0, true), 10_000));
    }

    #[test]
    fn inspected_groups_with_rows_are_fast_without_rows_slow() {
        assert!(needs_refresh(&Classification::MustInspect, fresh(0, true), LIVE_TTL_MS));

        assert!(!needs_refresh(&Classification::MustInspect, fresh(0, false), IDLE_TTL_MS - 1));
        assert!(needs_refresh(&Classification::MustInspect, fresh(0, false), IDLE_TTL_MS));
    }

    #[test]
    fn cache_round_trips_and_evicts_vanished_groups_and_ancient_entries() {
        let cache = GroupLagCache::new();
        cache.insert("t", "g1", CachedEntry { partitions: vec![lag_row()], sampled_at: 100 });
        cache.insert("t", "g2", CachedEntry { partitions: vec![], sampled_at: 100 });
        cache.insert("other", "g3", CachedEntry { partitions: vec![], sampled_at: 100 });
        assert_eq!(cache.get("t", "g1").unwrap().sampled_at, 100);
        assert!(cache.get("t", "missing").is_none());
        assert!(cache.freshness("t", "g1").unwrap().has_rows);
        assert!(!cache.freshness("t", "g2").unwrap().has_rows);

        // g2 vanished from the group list; g3 belongs to another topic and
        // must survive a "t" eviction pass
        cache.evict("t", &|g| g == "g1", 200);
        assert!(cache.get("t", "g1").is_some());
        assert!(cache.get("t", "g2").is_none());
        assert!(cache.get("other", "g3").is_some());

        // any entry older than the global horizon dies, topic irrelevant
        cache.evict("t", &|_| true, 100 + EVICT_AGE_MS);
        assert!(cache.get("t", "g1").is_none());
        assert!(cache.get("other", "g3").is_none());
    }

    /// Two polls of the same topic overlap: the one holding the OLDER
    /// group-list snapshot must not delete an entry a newer request just
    /// wrote for a group its own snapshot never saw.
    #[test]
    fn eviction_keeps_entries_newer_than_the_callers_snapshot() {
        let cache = GroupLagCache::new();
        cache.insert("t", "newcomer", CachedEntry { partitions: vec![lag_row()], sampled_at: 500 });
        cache.evict("t", &|g| g == "known", 400);
        assert!(
            cache.get("t", "newcomer").is_some(),
            "an entry sampled after the caller's snapshot belongs to a newer view"
        );
        // ...but once the caller's own snapshot is the newer one, it rules
        cache.evict("t", &|g| g == "known", 600);
        assert!(cache.get("t", "newcomer").is_none());
    }
}
