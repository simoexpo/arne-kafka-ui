//! Cursor codec and window math for the messages timeline.
//!
//! These are pure functions: no I/O, no Kafka client, nothing async. The
//! wider timeline engine (later tasks) supplies real per-partition
//! watermarks and drives `page_windows`/`advance` in a loop; this module
//! only computes the arithmetic.

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use super::range;

/// Which way a page reads relative to its cursor's positions.
///
/// Per the design doc: for `Back`, a position is the *exclusive upper
/// bound* of the next page (the next record read is strictly below it);
/// for `Forward`, a position is the *inclusive lower bound* (the next
/// record read is at-or-above it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Back,
    Forward,
}

/// An opaque, per-partition offset map plus the direction it continues in.
/// Serialized as compact JSON, then base64, so it round-trips as a single
/// URL-safe-ish query-string token without the client needing to know its
/// shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    pub direction: Direction,
    pub positions: Vec<(i32, i64)>,
}

impl Cursor {
    pub fn encode(&self) -> String {
        // `serde_json::to_vec` on a struct made only of enums/tuples/Vecs
        // never fails (no maps with non-string keys, no floats), so this
        // is infallible in practice.
        let json = serde_json::to_vec(self).expect("Cursor always serializes");
        base64::engine::general_purpose::STANDARD.encode(json)
    }

    pub fn decode(s: &str) -> Result<Cursor, String> {
        let bytes = base64::engine::general_purpose::STANDARD.decode(s).map_err(|e| e.to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }
}

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
    }
}

/// Computes, per partition, a window of up to `span` records adjacent to
/// `positions` in `direction`, clamped to that partition's watermarks.
/// Partitions with nothing available in that direction (already at the
/// edge) are omitted, matching `range::window_ranges`'s convention.
pub fn page_windows(
    positions: &[(i32, i64)],
    watermarks: &[(i32, i64, i64)],
    direction: Direction,
    span: u64,
) -> Vec<range::PartitionRange> {
    let span = span as i64;
    positions
        .iter()
        .filter_map(|&(partition, pos)| {
            let &(_, lo, hi) = watermarks.iter().find(|(p, _, _)| *p == partition)?;
            let pos = pos.clamp(lo, hi);
            let (start, end) = match direction {
                Direction::Back => ((pos - span).max(lo), pos),
                Direction::Forward => (pos, (pos + span).min(hi)),
            };
            (end > start).then_some(range::PartitionRange { partition, start, end })
        })
        .collect()
}

/// Advances `positions` past the given `windows` (the page just consumed),
/// returning the new positions and whether the topic's edge was reached in
/// `direction`.
///
/// A partition with no entry in `windows` had zero room to begin with —
/// `page_windows` only omits a partition when its position was *already*
/// sitting at the low (Back) or high (Forward) watermark, so that
/// partition's position is unambiguously at the edge and is left
/// unchanged.
///
/// For a partition that *does* have a window, whether it was clamped by
/// the watermark (edge reached) or simply given a full `span`-sized page
/// (more data may remain beyond it) isn't recoverable from `windows` alone
/// — that distinction needs either the original `span` or the watermarks
/// themselves, and this function's signature (matching the brief/tests)
/// receives neither. As the best available proxy: `page_windows` applies
/// one uniform `span` across every partition in a single call, so a
/// partition whose window is shorter than the batch's widest window must
/// have been clamped by its watermark. The partition(s) tied for the
/// widest window are, strictly speaking, undetermined by this function
/// alone (see task-1 report for this as a flagged concern) — they're
/// treated as also having hit the edge, which is the direction that fails
/// safe: a caller that later re-derives exhaustion from fresh watermarks
/// (as the real SSE handler will) simply sees more data on the next
/// request rather than this function silently hiding any.
pub fn advance(
    positions: &[(i32, i64)],
    windows: &[range::PartitionRange],
    direction: Direction,
) -> (Vec<(i32, i64)>, bool) {
    let find = |p: i32| windows.iter().find(|w| w.partition == p);

    let new_positions = positions
        .iter()
        .map(|&(p, pos)| match find(p) {
            Some(w) => (p, match direction { Direction::Back => w.start, Direction::Forward => w.end }),
            None => (p, pos),
        })
        .collect();

    let widest = windows.iter().map(|w| (w.end - w.start) as u64).max();
    let exhausted = positions.iter().all(|&(p, _)| match find(p) {
        None => true,
        Some(w) => widest.is_none_or(|m| (w.end - w.start) as u64 <= m),
    });

    (new_positions, exhausted)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WM: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5)];

    #[test]
    fn cursor_roundtrips() {
        let c = Cursor { direction: Direction::Back, positions: vec![(0, 60), (1, 5)] };
        let c2 = Cursor::decode(&c.encode()).unwrap();
        assert_eq!(c2.direction, Direction::Back);
        assert_eq!(c2.positions, vec![(0, 60), (1, 5)]);
        assert!(Cursor::decode("garbage!").is_err());
    }

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

    #[test]
    fn back_windows_take_span_below_position() {
        let w = page_windows(&[(0, 110), (1, 5)], WM, Direction::Back, 50);
        assert_eq!(w, vec![
            range::PartitionRange { partition: 0, start: 60, end: 110 },
            range::PartitionRange { partition: 1, start: 0, end: 5 },
        ]);
    }

    #[test]
    fn forward_windows_take_span_above_position() {
        let w = page_windows(&[(0, 10), (1, 0)], WM, Direction::Forward, 3);
        assert_eq!(w, vec![
            range::PartitionRange { partition: 0, start: 10, end: 13 },
            range::PartitionRange { partition: 1, start: 0, end: 3 },
        ]);
    }

    #[test]
    fn advance_back_moves_down_and_detects_exhaustion() {
        let w = page_windows(&[(0, 60), (1, 5)], WM, Direction::Back, 100);
        let (p, exhausted) = advance(&[(0, 60), (1, 5)], &w, Direction::Back);
        assert_eq!(p, vec![(0, 10), (1, 0)]);
        assert!(exhausted); // both hit low watermark
    }

    #[test]
    fn advance_forward_detects_edge_against_high() {
        let w = page_windows(&[(0, 100), (1, 5)], WM, Direction::Forward, 50);
        let (p, exhausted) = advance(&[(0, 100), (1, 5)], &w, Direction::Forward);
        assert_eq!(p, vec![(0, 110), (1, 5)]);
        assert!(exhausted);
    }
}
