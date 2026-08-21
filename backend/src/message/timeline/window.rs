use crate::message::range::{self, PartitionRange};

use super::cursor::Direction;

/// Computes, per partition, a window of up to `span` records adjacent to
/// `positions` in `direction`, clamped to that partition's watermarks.
/// Partitions with nothing available in that direction (already at the
/// edge) are omitted — the resulting `range::PartitionRange`s always have
/// `end > start`.
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
            (end > start).then_some(range::PartitionRange {
                partition,
                start,
                end,
            })
        })
        .collect()
}

/// True if `pos` sits at the topic edge in `direction` for a partition whose
/// watermarks are `[lo, hi]` — the low watermark for `Back`, the high
/// watermark for `Forward`. Shared by `run_page`'s active-partition filter
/// and its exhaustion check, so both agree on what "at the edge" means.
pub(super) fn at_edge(pos: i64, lo: i64, hi: i64, direction: Direction) -> bool {
    match direction {
        Direction::Back => pos == lo,
        Direction::Forward => pos == hi,
    }
}

/// Hardens decoded cursor positions before they drive anything else:
/// clamps each into its partition's *current* `[lo, hi]` watermark range,
/// and drops any partition that has no watermark entry at all. `run_page`
/// calls this first, so neither `page_windows` nor the cursor encoder ever
/// sees a raw position.
///
/// **Clamping.** A forged cursor, or a legitimate one whose partition has
/// since been trimmed by retention, can carry a position outside today's
/// watermarks (e.g. an offset below the new low watermark). `page_windows`
/// clamps its own internal `pos` when computing window bounds — but a
/// partition that gets no window this page (already at its edge) keeps its
/// position unchanged, so that position must itself already equal `lo`/`hi`
/// to be recognized as at-edge on the next comparison. An unclamped
/// out-of-range value never matches the edge check (`pos == lo`/`hi`), so
/// the same non-advancing cursor would be re-encoded and handed back
/// forever: identical cursor, `exhausted: false`, nothing new, no error — a
/// silent infinite loop from the client's point of view. Clamping up front
/// makes "already at the edge" actually equal `lo`/`hi`.
///
/// **Dropping unknown partitions.** A partition id with *no* watermark
/// entry isn't a clamping problem (there's nothing to clamp against) — it
/// means the partition doesn't exist in this topic today. Partition counts
/// only ever grow for a topic, so this is either a forged cursor or one
/// carried over from a different topic; with no valid position to fall back
/// to, it is dropped from the tracked positions rather than carried through.
/// This matters beyond cosmetics: a phantom partition has no watermark to
/// compare against, so `run_page`'s exhaustion check (which requires *every*
/// tracked partition to be at its edge) could never be satisfied for it —
/// the page's `exhausted` would be stuck at `false` forever, even once every
/// real partition genuinely finished.
pub(super) fn clamp_positions(
    positions: &[(i32, i64)],
    watermarks: &[(i32, i64, i64)],
) -> Vec<(i32, i64)> {
    positions
        .iter()
        .filter_map(|&(partition, pos)| {
            watermarks
                .iter()
                .find(|(p, _, _)| *p == partition)
                .map(|&(_, lo, hi)| (partition, pos.clamp(lo, hi)))
        })
        .collect()
}

/// The offset, within a partition's window `w`, that sits immediately
/// adjacent to the position this page started from — the very first record
/// `run_page`'s contiguous selection would consume from that partition.
///
/// For `Back`, the position is the window's *exclusive upper bound*
/// (`w.end`), so the adjacent record is the highest offset in the window,
/// `w.end - 1` — the *top* of the window. For `Forward`, the position is
/// the window's *inclusive lower bound* (`w.start`), so the adjacent record
/// is `w.start` itself — the *bottom*. `select_partition_streams`' short-read
/// guard looks for exactly this offset among an incomplete partition's
/// fetched records to decide whether the whole window can be trusted.
pub(super) fn adjacent_offset(w: &PartitionRange, direction: Direction) -> i64 {
    match direction {
        Direction::Back => w.end - 1,
        Direction::Forward => w.start,
    }
}

/// Truncates `windows` — kept in their original order, which is
/// `cur_positions`' own stable order — to a prefix whose total span does
/// not exceed `budget_cap`, dropping any windows past that point entirely
/// for this chunk. A dropped partition simply keeps its current position
/// this iteration, exactly like any partition `run_page`'s cursor math
/// already finds absent from `windows` (already-at-edge partitions get the
/// same fallback).
///
/// Needed because `per_partition_share`'s `.max(1)` floor — reached once
/// `remaining_budget < active_partitions` — otherwise lets one chunk's
/// *potential* charge (`span * windows.len()`) exceed the real remaining
/// budget by up to `windows.len() - 1` records: with 3 partitions and 1
/// record of budget left, a naive per-partition span of 1 each would spend
/// 3, not 1. The scan budget is a hard per-request cap, not an approximate
/// target — `chunk_scanned` (bounded above by each window's own span) must
/// never be able to exceed what was actually left.
pub(super) fn cap_windows_to_budget(
    windows: Vec<PartitionRange>,
    budget_cap: u64,
) -> Vec<PartitionRange> {
    let mut total = 0u64;
    let mut out = Vec::with_capacity(windows.len());
    for w in windows {
        let span = (w.end - w.start) as u64;
        if total + span > budget_cap {
            break;
        }
        total += span;
        out.push(w);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::timeline::anchor::{Anchor, initial_positions};

    const WM: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5)];

    #[test]
    fn back_windows_take_span_below_position() {
        let w = page_windows(&[(0, 110), (1, 5)], WM, Direction::Back, 50);
        assert_eq!(
            w,
            vec![
                range::PartitionRange {
                    partition: 0,
                    start: 60,
                    end: 110
                },
                range::PartitionRange {
                    partition: 1,
                    start: 0,
                    end: 5
                },
            ]
        );
    }

    #[test]
    fn forward_windows_take_span_above_position() {
        let w = page_windows(&[(0, 10), (1, 0)], WM, Direction::Forward, 3);
        assert_eq!(
            w,
            vec![
                range::PartitionRange {
                    partition: 0,
                    start: 10,
                    end: 13
                },
                range::PartitionRange {
                    partition: 1,
                    start: 0,
                    end: 3
                },
            ]
        );
    }

    /// Guards the bound semantics away from the watermarks. Every other
    /// `page_windows` test positions `p` exactly ON a watermark, where
    /// `.clamp(lo, hi)` can mask an off-by-one in the window arithmetic
    /// itself; this one uses a genuinely mid-range position (lo=0, hi=100,
    /// p=60, span=10, nowhere near either watermark), so only the
    /// arithmetic can satisfy it: `Back` yields the exclusive-upper window
    /// `[50, 60)` (offsets ≤ 59), `Forward` the inclusive-lower window
    /// `[60, 70)` (offsets ≥ 60).
    #[test]
    fn page_windows_mid_range_position_back_and_forward() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 100)];
        let back = page_windows(&[(0, 60)], wm, Direction::Back, 10);
        assert_eq!(
            back,
            vec![range::PartitionRange {
                partition: 0,
                start: 50,
                end: 60
            }]
        );
        let forward = page_windows(&[(0, 60)], wm, Direction::Forward, 10);
        assert_eq!(
            forward,
            vec![range::PartitionRange {
                partition: 0,
                start: 60,
                end: 70
            }]
        );
    }

    #[test]
    fn back_window_reaching_the_low_watermark_is_flagged_at_edge() {
        let w = page_windows(&[(0, 60), (1, 5)], WM, Direction::Back, 100);
        assert_eq!(
            w,
            vec![
                range::PartitionRange {
                    partition: 0,
                    start: 10,
                    end: 60
                },
                range::PartitionRange {
                    partition: 1,
                    start: 0,
                    end: 5
                },
            ]
        );
        // consuming each window fully lands exactly on its low watermark —
        // this is the boundary `run_page`'s cursor math advances to.
        assert!(at_edge(w[0].start, 10, 110, Direction::Back));
        assert!(at_edge(w[1].start, 0, 5, Direction::Back));
    }

    #[test]
    fn forward_window_reaching_the_high_watermark_is_flagged_at_edge() {
        let w = page_windows(&[(0, 100), (1, 5)], WM, Direction::Forward, 50);
        // partition 1 is already at its high watermark: page_windows omits it entirely
        assert_eq!(
            w,
            vec![range::PartitionRange {
                partition: 0,
                start: 100,
                end: 110
            }]
        );
        assert!(at_edge(w[0].end, 10, 110, Direction::Forward));
        assert!(
            at_edge(5, 0, 5, Direction::Forward),
            "partition 1's untouched position is already at its edge"
        );
    }

    #[test]
    fn first_page_of_deep_partition_leaves_its_window_short_of_the_low_watermark() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 1_000_000)];
        let positions = initial_positions(wm, &Anchor::Latest);
        let w = page_windows(&positions, wm, Direction::Back, 100);
        assert_eq!(
            w,
            vec![range::PartitionRange {
                partition: 0,
                start: 999_900,
                end: 1_000_000
            }]
        );
        assert!(
            !at_edge(w[0].start, 0, 1_000_000, Direction::Back),
            "plenty of room left below this window"
        );
    }

    #[test]
    fn imbalanced_partitions_reach_their_low_watermark_independently() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 1000), (1, 0, 5)];
        let positions = initial_positions(wm, &Anchor::Latest);
        let w = page_windows(&positions, wm, Direction::Back, 100);
        assert_eq!(
            w,
            vec![
                range::PartitionRange {
                    partition: 0,
                    start: 900,
                    end: 1000
                },
                range::PartitionRange {
                    partition: 1,
                    start: 0,
                    end: 5
                },
            ]
        );
        assert!(
            !at_edge(w[0].start, 0, 1000, Direction::Back),
            "partition 0 still has plenty of room left"
        );
        assert!(
            at_edge(w[1].start, 0, 5, Direction::Back),
            "partition 1's window already reaches its low watermark"
        );

        // continuing from partition 0's new position with a big-enough span reaches its edge too
        let w2 = page_windows(&[(0, w[0].start)], wm, Direction::Back, 1000);
        assert_eq!(
            w2,
            vec![range::PartitionRange {
                partition: 0,
                start: 0,
                end: 900
            }]
        );
        assert!(at_edge(w2[0].start, 0, 1000, Direction::Back));
    }

    /// The anti-infinite-loop property behind clamping: a forged or
    /// retention-raced cursor position (-5) on watermarks (0, 0, 10) must be
    /// clamped before it drives `page_windows` — otherwise a partition with
    /// no window this page would keep the raw, unclamped -5 forever:
    /// `-5 == lo (0)` never holds, so it's never recognized as being at the
    /// edge and the identical cursor loops forever. Clamped to 0 first, the
    /// partition is correctly recognized as already at its low watermark.
    #[test]
    fn clamped_position_reaches_exhaustion_instead_of_looping() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 10)];
        let forged = vec![(0, -5)];

        let positions = clamp_positions(&forged, wm);
        assert_eq!(
            positions,
            vec![(0, 0)],
            "position must be clamped into [lo, hi]"
        );

        let windows = page_windows(&positions, wm, Direction::Back, 5);
        assert!(
            windows.is_empty(),
            "already at the low watermark: nothing to fetch"
        );

        // a partition with no window keeps its (already-clamped) position
        // unchanged, so that position itself must already be at the edge —
        // otherwise the unchanged cursor would be re-encoded and handed back
        // forever.
        assert!(
            at_edge(positions[0].1, 0, 10, Direction::Back),
            "clamped position at the edge must report exhausted, not loop forever"
        );
    }

    /// Anchor partition property (binding acceptance test),
    /// cheap variant: for `Anchor::Beginning`, `back(anchor)` has nothing to
    /// read (already at the low watermark) while `forward(anchor)` reads
    /// everything — a trivial but real instance of "back(anchor) and
    /// forward(anchor) split the topic disjointly and completely per
    /// partition". Pure, no broker: `initial_positions` + `page_windows`
    /// alone already guarantee this, so it's tested here rather than as a
    /// real-cluster integration test (the timestamp-anchor case, which
    /// isn't this cheap, gets that treatment in `tests/api.rs`).
    #[test]
    fn anchor_beginning_property_back_empty_forward_everything() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 20), (1, 0, 20)];
        let positions = initial_positions(wm, &Anchor::Beginning);
        assert_eq!(positions, vec![(0, 0), (1, 0)]);

        let back = page_windows(&positions, wm, Direction::Back, 500);
        assert!(
            back.is_empty(),
            "back(beginning) must fetch nothing: already at the low watermark"
        );

        let forward = page_windows(&positions, wm, Direction::Forward, 500);
        assert_eq!(
            forward,
            vec![
                range::PartitionRange {
                    partition: 0,
                    start: 0,
                    end: 20
                },
                range::PartitionRange {
                    partition: 1,
                    start: 0,
                    end: 20
                },
            ],
            "forward(beginning) must cover the entire topic"
        );
    }

    /// Symmetric case: `Anchor::Latest` — `forward(anchor)` has nothing to
    /// read (already at the high watermark), `back(anchor)` reads
    /// everything.
    #[test]
    fn anchor_latest_property_forward_empty_back_everything() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 20), (1, 0, 20)];
        let positions = initial_positions(wm, &Anchor::Latest);
        assert_eq!(positions, vec![(0, 20), (1, 20)]);

        let forward = page_windows(&positions, wm, Direction::Forward, 500);
        assert!(
            forward.is_empty(),
            "forward(latest) must fetch nothing: already at the high watermark"
        );

        let back = page_windows(&positions, wm, Direction::Back, 500);
        assert_eq!(
            back,
            vec![
                range::PartitionRange {
                    partition: 0,
                    start: 0,
                    end: 20
                },
                range::PartitionRange {
                    partition: 1,
                    start: 0,
                    end: 20
                },
            ],
            "back(latest) must cover the entire topic"
        );
    }

    /// Sanity check: a position already inside range is left untouched.
    #[test]
    fn clamp_positions_is_a_no_op_in_range() {
        let wm: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5)];
        let positions = vec![(0, 60), (1, 3)];
        assert_eq!(clamp_positions(&positions, wm), positions);
    }

    /// The dropping half: a cursor decoded to
    /// `[(0, 0), (99, 5)]` on a 1-partition topic (watermarks only cover
    /// partition 0) must behave as `[(0, 0)]` — partition 99 doesn't exist,
    /// so it's dropped rather than carried through with no watermark to
    /// clamp or exhaust it against.
    #[test]
    fn clamp_positions_drops_unknown_partitions() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 10)];
        let positions = vec![(0, 0), (99, 5)];
        assert_eq!(clamp_positions(&positions, wm), vec![(0, 0)]);
    }

    #[test]
    fn adjacent_offset_back_is_top_of_window() {
        let w = range::PartitionRange {
            partition: 0,
            start: 5,
            end: 10,
        };
        assert_eq!(adjacent_offset(&w, Direction::Back), 9);
    }

    #[test]
    fn adjacent_offset_forward_is_bottom_of_window() {
        let w = range::PartitionRange {
            partition: 0,
            start: 5,
            end: 10,
        };
        assert_eq!(adjacent_offset(&w, Direction::Forward), 5);
    }
}
