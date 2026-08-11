use serde::Serialize;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct PartitionRange {
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

pub fn latest_ranges(watermarks: &[(i32, i64, i64)], limit: u64) -> Vec<PartitionRange> {
    watermarks
        .iter()
        .filter(|(_, lo, hi)| hi > lo)
        .map(|&(p, lo, hi)| PartitionRange { partition: p, start: (hi - limit as i64).max(lo), end: hi })
        .collect()
}

pub fn offset_range(watermarks: &[(i32, i64, i64)], partition: i32, from: i64, limit: u64) -> Option<PartitionRange> {
    let &(p, lo, hi) = watermarks.iter().find(|(p, _, _)| *p == partition)?;
    let start = from.clamp(lo, hi);
    let end = (start + limit as i64).min(hi);
    Some(PartitionRange { partition: p, start, end })
}

pub fn window_ranges(
    watermarks: &[(i32, i64, i64)],
    starts: &[(i32, Option<i64>)],
    end_exclusive: Option<&[(i32, i64)]>,
) -> Vec<PartitionRange> {
    starts
        .iter()
        .filter_map(|&(partition, start)| {
            let start = start?;
            let &(_, lo, hi) = watermarks.iter().find(|(p, _, _)| *p == partition)?;
            let end = end_exclusive
                .and_then(|ends| ends.iter().find(|(p, _)| *p == partition).map(|&(_, e)| e))
                .unwrap_or(hi)
                .min(hi);
            let start = start.clamp(lo, hi);
            (end > start).then_some(PartitionRange { partition, start, end })
        })
        .collect()
}

pub fn total(ranges: &[PartitionRange]) -> u64 {
    ranges.iter().map(|r| (r.end - r.start) as u64).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    const WM: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5), (2, 40, 40)];

    #[test]
    fn latest_takes_last_n_clamped_to_low() {
        let r = latest_ranges(WM, 50);
        assert_eq!(r, vec![
            PartitionRange { partition: 0, start: 60, end: 110 },
            PartitionRange { partition: 1, start: 0, end: 5 },   // fewer than 50 available
            // partition 2 empty → omitted
        ]);
    }

    #[test]
    fn offset_range_clamps_and_rejects_unknown_partition() {
        assert_eq!(offset_range(WM, 0, 100, 50), Some(PartitionRange { partition: 0, start: 100, end: 110 }));
        assert_eq!(offset_range(WM, 0, 5, 20), Some(PartitionRange { partition: 0, start: 10, end: 30 })); // clamped up to low
        assert_eq!(offset_range(WM, 9, 0, 10), None);
    }

    #[test]
    fn window_defaults_end_to_high_watermark() {
        let r = window_ranges(WM, &[(0, Some(100)), (1, Some(2)), (2, None)], None);
        assert_eq!(r, vec![
            PartitionRange { partition: 0, start: 100, end: 110 },
            PartitionRange { partition: 1, start: 2, end: 5 },
        ]);
    }

    #[test]
    fn window_respects_explicit_ends() {
        let r = window_ranges(WM, &[(0, Some(60))], Some(&[(0, 80)]));
        assert_eq!(r, vec![PartitionRange { partition: 0, start: 60, end: 80 }]);
    }

    #[test]
    fn total_sums_spans() {
        let r = vec![
            PartitionRange { partition: 0, start: 60, end: 110 },
            PartitionRange { partition: 1, start: 0, end: 5 },
        ];
        assert_eq!(total(&r), 55);
    }
}
