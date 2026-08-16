/// Where a fresh (non-cursor) page request starts from.
#[derive(Debug, Clone, PartialEq)]
pub enum Anchor {
    /// Start at the high watermark of every partition (open the tab: latest
    /// N, live prepend on).
    Latest,
    /// Start at the low watermark of every partition (jump to beginning).
    Beginning,
    /// Jump to a specific message. Only the named partition is positioned
    /// at that message; every other partition starts at its own high
    /// watermark.
    ///
    /// This is a deliberate simplification, not an oversight: an offset
    /// anchor is a *partition-local* jump (the user picked one message in
    /// one partition), and there's no principled cross-partition offset to
    /// derive from it — a message's timestamp doesn't imply a comparable
    /// offset in another partition. The global timeline is produced by the
    /// merge-sort-by-timestamp step downstream, which is what actually
    /// reconciles the anchored partition's neighborhood against everyone
    /// else's tail; `initial_positions` just has to give every partition a
    /// *valid* starting point.
    Offset { partition: i32, offset: i64 },
    /// Timestamp anchor, already resolved upstream (one Kafka
    /// `OffsetsForTimes` call per partition) to either the offset of the
    /// first message at-or-after the timestamp, or `None` if no such
    /// message exists (the timestamp is past the newest message in that
    /// partition). `None` is treated the same as "nothing more to find
    /// here": position at the high watermark, matching `Latest`'s
    /// behavior for that partition.
    TimestampResolved(Vec<(i32, Option<i64>)>),
    /// Offset anchor, FORWARD-direction alignment (owner ruling
    /// 2026-08-15 — supersedes `Offset`'s "pin everyone else at their high
    /// watermark" behavior for a forward read only; `Offset` itself is
    /// unchanged and still used for `direction=back`, see its own doc
    /// comment). The anchored `(partition, offset)` message's own
    /// timestamp is resolved upstream (one bounded single-record fetch),
    /// then every OTHER partition is resolved via the same
    /// `OffsetsForTimes` machinery `TimestampResolved` uses, at that same
    /// timestamp — `aligned` carries that per-partition result (`None` =
    /// nothing at/after the timestamp there: high watermark, matching
    /// `Latest`). The anchored partition itself is pinned to exactly
    /// `offset`, never re-derived from the timestamp lookup (`aligned` may
    /// also contain an entry for it, from the shared resolution call — it
    /// is simply ignored below): the whole point is that partition reads
    /// forward from the EXACT message the user picked, not a second
    /// approximation of it. This is what makes the anchored message the
    /// OLDEST row of its own partition in the resulting forward page,
    /// while every other partition picks up at-or-after the same instant —
    /// "nothing lost, nothing pinned to the wrong end" relative to a
    /// `TimestampResolved` anchor at that same timestamp.
    OffsetForwardAligned { partition: i32, offset: i64, aligned: Vec<(i32, Option<i64>)> },
}

/// Computes the starting cursor positions for a fresh (non-cursor) page
/// request. Returned in `watermarks`' partition order.
pub fn initial_positions(watermarks: &[(i32, i64, i64)], anchor: &Anchor) -> Vec<(i32, i64)> {
    match anchor {
        Anchor::Latest => watermarks.iter().map(|&(p, _, hi)| (p, hi)).collect(),
        Anchor::Beginning => watermarks.iter().map(|&(p, lo, _)| (p, lo)).collect(),
        Anchor::Offset { partition, offset } => watermarks
            .iter()
            .map(|&(p, _, hi)| if p == *partition { (p, offset + 1) } else { (p, hi) })
            .collect(),
        Anchor::TimestampResolved(resolved) => watermarks
            .iter()
            .map(|&(p, _, hi)| {
                let found = resolved.iter().find(|&&(rp, _)| rp == p).and_then(|&(_, o)| o);
                (p, found.unwrap_or(hi))
            })
            .collect(),
        Anchor::OffsetForwardAligned { partition, offset, aligned } => watermarks
            .iter()
            .map(|&(p, _, hi)| {
                if p == *partition {
                    (p, *offset)
                } else {
                    let found = aligned.iter().find(|&&(rp, _)| rp == p).and_then(|&(_, o)| o);
                    (p, found.unwrap_or(hi))
                }
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WM: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5)];

    #[test]
    fn latest_positions_are_high_watermarks() {
        let p = initial_positions(WM, &Anchor::Latest);
        assert_eq!(p, vec![(0, 110), (1, 5)]);
    }

    #[test]
    fn beginning_positions_are_low_watermarks() {
        let p = initial_positions(WM, &Anchor::Beginning);
        assert_eq!(p, vec![(0, 10), (1, 0)]);
    }

    /// Owner ruling 2026-08-15: a forward offset anchor pins the anchored
    /// partition at the EXACT offset (not `aligned`'s own resolution for
    /// it, even if present) and every other partition at its aligned
    /// timestamp-resolved offset — `None` there falls back to the high
    /// watermark, exactly like `TimestampResolved`.
    #[test]
    fn offset_forward_aligned_pins_anchor_exactly_and_aligns_others() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 100), (1, 0, 100), (2, 0, 100)];
        let anchor = Anchor::OffsetForwardAligned {
            partition: 0,
            offset: 42,
            // Partition 0's own entry (55) must be IGNORED in favor of the
            // exact `offset` (42); partition 1 aligns to 30; partition 2 has
            // nothing at/after the timestamp (falls back to its hi, 100).
            aligned: vec![(0, Some(55)), (1, Some(30)), (2, None)],
        };
        let p = initial_positions(wm, &anchor);
        assert_eq!(p, vec![(0, 42), (1, 30), (2, 100)]);
    }
}
