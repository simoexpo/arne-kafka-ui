use crate::cluster::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::message::fetch;

use super::cursor::Direction;

/// Where a fresh (non-cursor) page request starts from.
#[derive(Debug, Clone, PartialEq)]
pub enum Anchor {
    /// Start at the high watermark of every partition (open the tab: latest
    /// N, live prepend on).
    Latest,
    /// Start at the low watermark of every partition (jump to beginning).
    Beginning,
    /// Jump to a specific message, used for `direction=back` offset jumps
    /// (a forward jump uses `OffsetForwardAligned` — see
    /// `api::messages::timeline_sse`). Only the named partition is
    /// positioned at that message (at `offset + 1`, since a `Back` position
    /// is an exclusive upper bound, so the anchored message itself is the
    /// first record read); every other partition starts at its own high
    /// watermark.
    ///
    /// Pinning the others at their high watermark is a deliberate
    /// simplification, not an oversight: an offset anchor is a
    /// *partition-local* jump (the user picked one message in one
    /// partition), and there's no principled cross-partition offset to
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
    /// Offset anchor for a FORWARD read (`direction=back` uses `Offset`
    /// instead — see `api::messages::timeline_sse`).
    ///
    /// Upstream resolves the anchored `(partition, offset)` message's own
    /// timestamp with one bounded single-record fetch
    /// (`fetch::fetch_one_record_blocking`), then resolves every partition
    /// at that timestamp through the same `OffsetsForTimes` call
    /// `TimestampResolved` uses; `aligned` carries that per-partition result
    /// (`None` = nothing at/after the timestamp in that partition, which
    /// falls back to its high watermark, matching `Latest`).
    ///
    /// The anchored partition is pinned to exactly `offset` and never
    /// re-derived from the timestamp lookup — `aligned` may carry an entry
    /// for it from the shared resolution call, and that entry is ignored.
    /// The point is that this partition reads forward from the EXACT message
    /// the user picked rather than a second approximation of it, which is
    /// what makes the anchored message the OLDEST row of its own partition
    /// in the resulting page while every other partition picks up at-or-after
    /// the same instant: nothing lost, nothing pinned to the wrong end,
    /// relative to a `TimestampResolved` anchor at that same timestamp.
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

/// A fresh (non-cursor) page request's anchor, already validated by the
/// caller (`api::messages::TimelineParams` parsing) but not yet resolved
/// against a cluster — resolving `Offset`/`Timestamp` into positions needs
/// watermarks and, for a forward offset anchor, a single-record fetch, both
/// of which are round trips `resolve_positions_blocking` below performs.
pub enum TimelineAnchorInput {
    Latest,
    Beginning,
    Offset { partition: i32, offset: i64 },
    Timestamp { ts_ms: i64 },
}

/// Resolves a timestamp to each partition's first offset at-or-after it, one
/// `OffsetsForTimes` round trip covering every partition in `watermarks`.
/// `None` for a partition means no message at/after `ts_ms` there (the
/// timestamp is past that partition's newest message).
pub fn resolve_timestamp_offsets_blocking(
    handle: &ClusterHandle,
    topic: &str,
    watermarks: &[(i32, i64, i64)],
    ts_ms: i64,
) -> Result<Vec<(i32, Option<i64>)>, ApiError> {
    use rdkafka::consumer::Consumer;
    use rdkafka::topic_partition_list::TopicPartitionList;
    use rdkafka::Offset;
    let mut tpl = TopicPartitionList::new();
    for &(p, _, _) in watermarks {
        tpl.add_partition_offset(topic, p, Offset::Offset(ts_ms))
            .map_err(|e| error::from_kafka(&handle.name, "offsets_for_times", &e))?;
    }
    let resolved = handle.consumer()
        .offsets_for_times(tpl, ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "offsets_for_times", &e))?;
    Ok(resolved.elements().iter().map(|e| {
        let start = match e.offset() {
            Offset::Offset(o) => Some(o),
            _ => None, // no message at/after ts in this partition
        };
        (e.partition(), start)
    }).collect())
}

/// Resolves a fresh page request's starting positions: builds the resolved
/// `Anchor` from the validated `TimelineAnchorInput` (which may need its own
/// Kafka round trips — a timestamp anchor resolves offsets-for-times, a
/// forward offset anchor additionally fetches the anchored message's own
/// timestamp first) and hands it to `initial_positions`. `watermarks` is the
/// caller's already-fetched watermarks round trip — nothing here re-fetches
/// them.
pub fn resolve_positions_blocking(
    handle: &ClusterHandle,
    topic: &str,
    watermarks: &[(i32, i64, i64)],
    input: TimelineAnchorInput,
    direction: Direction,
) -> Result<Vec<(i32, i64)>, ApiError> {
    let anchor = match input {
        TimelineAnchorInput::Latest => Anchor::Latest,
        TimelineAnchorInput::Beginning => Anchor::Beginning,
        TimelineAnchorInput::Offset { partition, offset } => {
            // Owner ruling 2026-08-15: `direction=back` keeps the original,
            // deliberately simpler `Offset` behavior (pins every other
            // partition at its high watermark — documented deferral, see
            // `Anchor::Offset`'s own doc comment). Only a forward read gets
            // the new alignment: resolve the anchored message's own
            // timestamp (a bounded single-record fetch — same consumer
            // discipline as everything else here) and align every other
            // partition at that instant, the same way a timestamp anchor
            // would.
            if direction == Direction::Forward {
                let anchor_ts = fetch::fetch_one_record_blocking(&handle.config, topic, partition, offset)?
                    .and_then(|r| r.timestamp_ms)
                    .ok_or_else(|| ApiError::bad_request(format!(
                        "no message with a timestamp at partition {partition} offset {offset}"
                    )))?;
                let aligned = resolve_timestamp_offsets_blocking(handle, topic, watermarks, anchor_ts)?;
                Anchor::OffsetForwardAligned { partition, offset, aligned }
            } else {
                Anchor::Offset { partition, offset }
            }
        }
        TimelineAnchorInput::Timestamp { ts_ms } => {
            let resolved = resolve_timestamp_offsets_blocking(handle, topic, watermarks, ts_ms)?;
            Anchor::TimestampResolved(resolved)
        }
    };
    Ok(initial_positions(watermarks, &anchor))
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

    /// A forward offset anchor pins the anchored partition at the EXACT
    /// offset (not `aligned`'s own resolution for it, even when present)
    /// and every other partition at its aligned timestamp-resolved offset —
    /// `None` there falls back to the high watermark, exactly like
    /// `TimestampResolved`.
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
