use crate::message::MessageOut;
use crate::message::fetch::RawRecord;

use super::cursor::Direction;

/// True if `candidate` should be selected over `current_best` as the next
/// record taken in `run_page`'s k-way merge (see its doc comment): `Back`
/// prefers the higher timestamp, `Forward` the lower. Ties break by
/// partition ascending — each partition contributes at most one candidate
/// per merge step, so a partition-level tie cannot arise.
pub(super) fn merge_prefers(
    direction: Direction,
    candidate: &RawRecord,
    current_best: &RawRecord,
) -> bool {
    let (ct, bt) = (
        candidate.timestamp_ms.unwrap_or(i64::MIN),
        current_best.timestamp_ms.unwrap_or(i64::MIN),
    );
    let ts_favors_candidate = match direction {
        Direction::Back => ct.cmp(&bt).is_gt(),
        Direction::Forward => ct.cmp(&bt).is_lt(),
    };
    if ct != bt {
        return ts_favors_candidate;
    }
    candidate.partition < current_best.partition
}

/// Ordering rule: within one
/// partition, offset order ALWAYS; across partitions, merge by timestamp
/// (ties: smaller partition id; null ts = i64::MIN). A pure-timestamp sort
/// would invert same-partition offset order under non-monotonic producer
/// timestamps — this k-way merge cannot, by construction. Mirrors the
/// frontend store's mergeRows so both sides agree on display order.
pub(super) fn chunk_display_order(
    matches: Vec<MessageOut>,
    direction: Direction,
) -> Vec<MessageOut> {
    let mut by_partition: std::collections::BTreeMap<i32, Vec<MessageOut>> =
        std::collections::BTreeMap::new();
    for m in matches {
        by_partition.entry(m.partition).or_default().push(m);
    }
    // pop() serves each stream's next head. Back's head is the HIGHEST
    // remaining offset — an offset-ascending vec already has it last.
    // Forward's head is the LOWEST — reverse the ascending vec so it's last.
    let mut streams: Vec<Vec<MessageOut>> = by_partition
        .into_values()
        .map(|mut v| {
            v.sort_by_key(|m| m.offset);
            if matches!(direction, Direction::Forward) {
                v.reverse();
            }
            v
        })
        .collect();
    let mut out = Vec::with_capacity(streams.iter().map(Vec::len).sum());
    loop {
        let mut best: Option<usize> = None;
        for (i, s) in streams.iter().enumerate() {
            let Some(head) = s.last() else { continue };
            let ts = head.timestamp_ms.unwrap_or(i64::MIN);
            let better = match best {
                None => true,
                Some(b) => {
                    let best_ts = streams[b].last().unwrap().timestamp_ms.unwrap_or(i64::MIN);
                    // Strict comparison: on ties the earlier stream (smaller
                    // partition id, BTreeMap order) wins.
                    match direction {
                        Direction::Back => ts > best_ts,
                        Direction::Forward => ts < best_ts,
                    }
                }
            };
            if better {
                best = Some(i);
            }
        }
        match best {
            Some(i) => out.push(streams[i].pop().expect("stream with a head")),
            None => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(partition: i32, offset: i64, ts: Option<i64>) -> RawRecord {
        RawRecord {
            partition,
            offset,
            timestamp_ms: ts,
            key: None,
            value: Some(b"x".to_vec()),
            headers: vec![],
        }
    }

    fn m(partition: i32, offset: i64, ts: Option<i64>) -> MessageOut {
        MessageOut {
            partition,
            offset,
            timestamp_ms: ts,
            key: None,
            value: None,
            headers: vec![],
        }
    }

    #[test]
    fn chunk_display_order_keeps_same_partition_offset_order_back() {
        // p0: offset 1 ts=100, offset 2 ts=50 (producer clock jumped back)
        // p1: offset 5 ts=80
        let matches = vec![m(0, 1, Some(100)), m(0, 2, Some(50)), m(1, 5, Some(80))];
        let out = chunk_display_order(matches, Direction::Back);
        let got: Vec<(i32, i64)> = out.iter().map(|x| (x.partition, x.offset)).collect();
        // newest-first merge: p0's head is offset 2 (ts 50), p1's head is offset 5
        // (ts 80). Pick p1 (80), then p0 offset 2 (50), then p0 offset 1 (100).
        // Same-partition offset order (2 before 1, descending) is preserved even
        // though ts order says otherwise; a pure-ts sort would emit (0,1) first.
        assert_eq!(got, vec![(1, 5), (0, 2), (0, 1)]);
    }

    #[test]
    fn chunk_display_order_forward_and_ties() {
        // Forward: oldest-first. Tie on ts=70 between p0 and p2 → smaller
        // partition id wins. Null ts sorts as MIN (first, forward).
        let matches = vec![
            m(2, 9, Some(70)),
            m(0, 3, Some(70)),
            m(1, 4, None),
            m(0, 4, Some(60)),
        ];
        let out = chunk_display_order(matches, Direction::Forward);
        let got: Vec<(i32, i64)> = out.iter().map(|x| (x.partition, x.offset)).collect();
        assert_eq!(got, vec![(1, 4), (0, 3), (0, 4), (2, 9)]);
    }

    #[test]
    fn merge_prefers_back_picks_higher_timestamp() {
        let newer = raw(0, 1, Some(300));
        let older = raw(1, 1, Some(200));
        assert!(merge_prefers(Direction::Back, &newer, &older));
        assert!(!merge_prefers(Direction::Back, &older, &newer));
    }

    #[test]
    fn merge_prefers_forward_picks_lower_timestamp() {
        let older = raw(0, 1, Some(200));
        let newer = raw(1, 1, Some(300));
        assert!(merge_prefers(Direction::Forward, &older, &newer));
        assert!(!merge_prefers(Direction::Forward, &newer, &older));
    }
}
