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
pub const EVICT_AGE_MS: i64 = 600_000;

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

/// What one group has committed, as the cluster reports it: every partition
/// it holds an offset on, across every topic. ONE entry per group serves every
/// view — the consumers list sums it whole, a topic's tab filters it to that
/// topic (owner ruling 2026-08-20).
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Lag is a SUBTRACTION between a committed offset and a head, so the two
/// sides must be sampled together: pairing a cached commit with a NEWER head
/// invents lag that was never true (a caught-up consumer on a 10k msg/s topic
/// would look 80k behind after 8 seconds), and pairing it with an OLDER head
/// puts `committed > end` on screen, which cannot happen in Kafka. So the pair
/// is what gets stored and what ages as one — a stored row is old, never wrong.
#[derive(Clone)]
pub struct LagSnapshot {
    pub rows: Vec<PartitionLag>,
    /// Partitions the group has committed on whose head could not be read
    /// (a leader election, or offsets left behind on a deleted topic). Kept
    /// per partition so trouble on one topic cannot make another topic's tab
    /// give up: a view is incomplete only if ITS partitions are in here.
    pub unknown: Vec<(String, i32)>,
}

impl LagSnapshot {
    /// The total this snapshot can prove: the sum of the partitions that were
    /// read. With `unknown` non-empty it is a LOWER BOUND, never a total — a
    /// caller must disclose it as such, since unread partitions can only add.
    /// `None` means there is no position at all to be behind.
    pub fn statable_total(&self) -> Option<i64> {
        (!self.rows.is_empty()).then(|| self.rows.iter().map(|r| r.lag).sum())
    }

    /// Whether this group holds any offsets on `topic` — what decides which
    /// freshness tier a topic's tab applies to the shared entry.
    pub fn covers(&self, topic: &str) -> bool {
        self.rows.iter().any(|r| r.topic == topic)
            || self.unknown.iter().any(|(t, _)| t == topic)
    }

    /// The rows for one topic, or all of them for a cluster-wide view.
    pub fn narrowed(&self, only_topic: Option<&str>) -> Vec<PartitionLag> {
        self.rows
            .iter()
            .filter(|r| only_topic.is_none_or(|t| r.topic == t))
            .cloned()
            .collect()
    }

    /// Whether this view is missing a partition, so its total cannot be
    /// stated. Summing what we have would under-report with confidence.
    pub fn incomplete_for(&self, only_topic: Option<&str>) -> bool {
        self.unknown.iter().any(|(t, _)| only_topic.is_none_or(|want| t == want))
    }
}

/// How old a group's commits may be for the purpose at hand. The 8s/60s split
/// is the same policy as before, but it now applies to a SHARED entry at read
/// time rather than being baked into separate per-topic entries: a group that
/// shows nothing on the topic you're looking at is worth re-reading only once
/// a minute, even though a group that shows rows is worth 8 seconds.
/// `None` means "do not read this group at all" — not "read it rarely". An
/// infinite TTL would still fall through to a fetch on a cache miss, and an
/// OffsetFetch per moved-away group is the O(N) class this codebase took an
/// incident on.
pub fn commits_ttl(class: &Classification, shows_rows_here: bool) -> Option<i64> {
    match class {
        Classification::AssignedToTopic => Some(LIVE_TTL_MS),
        Classification::MustInspect if shows_rows_here => Some(LIVE_TTL_MS),
        Classification::MustInspect => Some(IDLE_TTL_MS),
        // Callers skip these before asking; this is the backstop.
        Classification::MovedAway => None,
    }
}

/// Lag for every partition in `commits`, plus the ones whose head could not be
/// read.
///
/// A partition with no readable head becomes an entry in `unknown` rather than
/// an error for the whole group: a leader election on one topic must not blank
/// another topic's tab, and a total that silently omits a partition would
/// under-report with confidence. Callers state "undetermined" for a view whose
/// own partitions are in there.
pub fn lag_rows(
    commits: &[CommittedOffset],
    head: impl Fn(&str, i32) -> Option<i64>,
) -> LagSnapshot {
    let mut rows = Vec::new();
    let mut unknown = Vec::new();
    for c in commits {
        match head(&c.topic, c.partition) {
            Some(end_offset) => rows.push(PartitionLag {
                topic: c.topic.clone(),
                partition: c.partition,
                committed_offset: c.offset,
                end_offset,
                lag: (end_offset - c.offset).max(0),
            }),
            None => unknown.push((c.topic.clone(), c.partition)),
        }
    }
    rows.sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));
    unknown.sort();
    LagSnapshot { rows, unknown }
}

/// The partitions `commits` covers, narrowed to one topic when asked — what a
/// caller hands to the watermark cache before computing lag.
pub fn partitions_of(commits: &[CommittedOffset], only_topic: Option<&str>) -> Vec<(String, i32)> {
    commits
        .iter()
        .filter(|c| only_topic.is_none_or(|t| c.topic == t))
        .map(|c| (c.topic.clone(), c.partition))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(topic: &str, partition: i32, offset: i64) -> CommittedOffset {
        CommittedOffset { topic: topic.into(), partition, offset }
    }

    #[test]
    fn a_total_is_summed_from_the_partitions_that_were_read() {
        let snap = lag_rows(&[commit("t", 0, 5), commit("t", 1, 8)], |_, p| Some(10 + i64::from(p)));
        assert_eq!(snap.statable_total(), Some(8));
    }

    // Owner ruling 2026-08-20: a lower bound beats "unknown" — the caller
    // renders it as `>= n` because the real total can only be higher.
    #[test]
    fn an_unreadable_partition_still_yields_the_bound_the_rest_proves() {
        let snap = lag_rows(&[commit("t", 0, 5), commit("t", 1, 8)], |_, p| (p == 0).then_some(12));
        assert_eq!(snap.statable_total(), Some(7));
        assert_eq!(snap.unknown.len(), 1);
    }

    #[test]
    fn a_group_that_committed_nothing_has_no_total_at_all() {
        assert_eq!(lag_rows(&[], |_, _| Some(10)).statable_total(), None);
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
    /// mid-rebalance.
    #[test]
    fn a_member_assigned_nothing_forces_inspection() {
        assert_eq!(classify(&[Some(vec![])], "t"), Classification::MustInspect);
        assert_eq!(
            classify(&[Some(vec![]), Some(vec!["other".into()])], "t"),
            Classification::MustInspect
        );
        assert_eq!(
            classify(&[Some(vec![]), Some(vec!["t".into()])], "t"),
            Classification::AssignedToTopic
        );
    }

    /// The tier policy, now applied to ONE shared entry at read time: a group
    /// showing rows on the topic you're looking at is worth 8s; one showing
    /// nothing there is worth a minute, even though the same entry may be
    /// serving another view at 8s.
    #[test]
    fn a_group_showing_nothing_here_is_worth_re_reading_only_once_a_minute() {
        assert_eq!(commits_ttl(&Classification::AssignedToTopic, false), Some(LIVE_TTL_MS));
        assert_eq!(commits_ttl(&Classification::AssignedToTopic, true), Some(LIVE_TTL_MS));
        assert_eq!(commits_ttl(&Classification::MustInspect, true), Some(LIVE_TTL_MS));
        assert_eq!(commits_ttl(&Classification::MustInspect, false), Some(IDLE_TTL_MS));
        const { assert!(IDLE_TTL_MS > LIVE_TTL_MS) };
    }

    /// A moved-away group must not be READ at all — an infinite TTL would
    /// still fall through to a fetch on a cache miss.
    #[test]
    fn a_moved_away_group_is_never_read() {
        assert_eq!(commits_ttl(&Classification::MovedAway, false), None);
        assert_eq!(commits_ttl(&Classification::MovedAway, true), None);
    }

    fn commits() -> Vec<CommittedOffset> {
        vec![
            CommittedOffset { topic: "orders".into(), partition: 0, offset: 10 },
            CommittedOffset { topic: "orders".into(), partition: 1, offset: 5 },
            CommittedOffset { topic: "users".into(), partition: 0, offset: 7 },
        ]
    }

    #[test]
    fn lag_is_the_head_minus_the_commit_across_every_topic() {
        let snap = lag_rows(&commits(), |_t, p| Some(20 + i64::from(p)));
        assert_eq!(snap.rows.len(), 3);
        assert!(snap.unknown.is_empty());
        assert_eq!(snap.rows.iter().map(|r| r.lag).sum::<i64>(), (20 - 10) + (21 - 5) + (20 - 7));
    }

    /// The whole point of one shared entry: a topic's tab reads the same
    /// commits and simply narrows them to its own partitions.
    #[test]
    fn narrowing_to_one_topic_yields_only_that_topics_rows() {
        let snap = lag_rows(&commits(), |_t, _p| Some(9));
        let narrowed = snap.narrowed(Some("users"));
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].topic, "users");
        assert_eq!(narrowed[0].lag, 2);
        assert!(snap.covers("users") && snap.covers("orders"));
        assert_eq!(partitions_of(&commits(), Some("users")), vec![("users".to_string(), 0)]);
        assert_eq!(partitions_of(&commits(), None).len(), 3);
    }

    #[test]
    fn a_commit_beyond_the_head_reads_as_zero_not_negative() {
        // retention or a reset can leave a commit ahead of what we read
        let snap = lag_rows(&commits(), |_t, _p| Some(3));
        assert!(snap.rows.iter().all(|r| r.lag >= 0));
    }

    /// A partition whose head could not be read is NAMED, not dropped and not
    /// fatal: the rows we have stay usable, and only the views covering that
    /// partition must withhold their total.
    #[test]
    fn an_unreadable_head_is_named_per_partition_not_fatal() {
        let snap = lag_rows(&commits(), |_t, p| if p == 1 { None } else { Some(50) });
        assert_eq!(snap.unknown, vec![("orders".to_string(), 1)]);
        assert_eq!(snap.rows.len(), 2, "the readable partitions still have rows");
        assert!(snap.incomplete_for(Some("orders")), "orders cannot state a total");
        assert!(!snap.incomplete_for(Some("users")), "users is unaffected by orders' trouble");
        assert!(snap.incomplete_for(None), "nor can the cluster-wide view");
    }

    #[test]
    fn rows_come_back_in_a_stable_order() {
        let snap = lag_rows(&commits(), |_t, _p| Some(99));
        let keys: Vec<(String, i32)> = snap.rows.iter().map(|r| (r.topic.clone(), r.partition)).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
