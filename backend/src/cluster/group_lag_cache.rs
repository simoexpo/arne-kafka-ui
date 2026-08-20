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
/// topic — because lag is no longer stored, it is computed from these commits
/// against the shared watermark cache at read time (owner ruling 2026-08-20).
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// How old a group's commits may be for the purpose at hand. The 8s/60s split
/// is the same policy as before, but it now applies to a SHARED entry at read
/// time rather than being baked into separate per-topic entries: a group that
/// shows nothing on the topic you're looking at is worth re-reading only once
/// a minute, even though a group that shows rows is worth 8 seconds.
pub fn commits_ttl(class: &Classification, shows_rows_here: bool) -> i64 {
    match class {
        Classification::AssignedToTopic => LIVE_TTL_MS,
        Classification::MustInspect if shows_rows_here => LIVE_TTL_MS,
        Classification::MustInspect => IDLE_TTL_MS,
        // callers skip MovedAway groups before asking; this arm only keeps
        // the match total
        Classification::MovedAway => IDLE_TTL_MS,
    }
}

/// Lag for the partitions in `commits`, optionally narrowed to one topic.
///
/// `head` is the shared watermark cache's answer for a partition, and `None`
/// there means "we could not read it" — a partition whose watermark fetch
/// failed. That is an error rather than a skipped row: silently omitting a
/// partition would under-report the group's lag, which is worse than saying
/// we don't know.
pub fn lag_rows(
    commits: &[CommittedOffset],
    only_topic: Option<&str>,
    head: impl Fn(&str, i32) -> Option<i64>,
) -> Result<Vec<PartitionLag>, String> {
    let mut out = Vec::new();
    for c in commits.iter().filter(|c| only_topic.is_none_or(|t| c.topic == t)) {
        let Some(end_offset) = head(&c.topic, c.partition) else {
            return Err(format!("no offset reported for {}/{}", c.topic, c.partition));
        };
        out.push(PartitionLag {
            topic: c.topic.clone(),
            partition: c.partition,
            committed_offset: c.offset,
            end_offset,
            lag: (end_offset - c.offset).max(0),
        });
    }
    out.sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));
    Ok(out)
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
        assert_eq!(commits_ttl(&Classification::AssignedToTopic, false), LIVE_TTL_MS);
        assert_eq!(commits_ttl(&Classification::AssignedToTopic, true), LIVE_TTL_MS);
        assert_eq!(commits_ttl(&Classification::MustInspect, true), LIVE_TTL_MS);
        assert_eq!(commits_ttl(&Classification::MustInspect, false), IDLE_TTL_MS);
        const { assert!(IDLE_TTL_MS > LIVE_TTL_MS) };
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
        let rows = lag_rows(&commits(), None, |_t, p| Some(20 + i64::from(p))).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.iter().map(|r| r.lag).sum::<i64>(), (20 - 10) + (21 - 5) + (20 - 7));
    }

    /// The whole point of one shared entry: a topic's tab reads the same
    /// commits and simply narrows them to its own partitions.
    #[test]
    fn narrowing_to_one_topic_yields_only_that_topics_rows() {
        let rows = lag_rows(&commits(), Some("users"), |_t, _p| Some(9)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].topic, "users");
        assert_eq!(rows[0].lag, 2);
        assert_eq!(partitions_of(&commits(), Some("users")), vec![("users".to_string(), 0)]);
        assert_eq!(partitions_of(&commits(), None).len(), 3);
    }

    #[test]
    fn a_commit_beyond_the_head_reads_as_zero_not_negative() {
        // retention or a reset can leave a commit ahead of what we read
        let rows = lag_rows(&commits(), Some("users"), |_t, _p| Some(3)).unwrap();
        assert_eq!(rows[0].lag, 0);
    }

    /// A partition whose watermark could not be read makes the group's lag
    /// UNKNOWN. Omitting the row would silently under-report the total, which
    /// is a confident wrong answer.
    #[test]
    fn an_unreadable_head_fails_the_group_rather_than_dropping_a_partition() {
        let err = lag_rows(&commits(), None, |_t, p| if p == 1 { None } else { Some(50) }).unwrap_err();
        assert!(err.contains("orders/1"), "the failing partition is named: {err}");
    }

    #[test]
    fn rows_come_back_in_a_stable_order() {
        let rows = lag_rows(&commits(), None, |_t, _p| Some(99)).unwrap();
        let keys: Vec<(String, i32)> = rows.iter().map(|r| (r.topic.clone(), r.partition)).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
