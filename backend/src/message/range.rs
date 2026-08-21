use serde::Serialize;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct PartitionRange {
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

pub fn total(ranges: &[PartitionRange]) -> u64 {
    ranges.iter().map(|r| (r.end - r.start) as u64).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_sums_spans() {
        let r = vec![
            PartitionRange {
                partition: 0,
                start: 60,
                end: 110,
            },
            PartitionRange {
                partition: 1,
                start: 0,
                end: 5,
            },
        ];
        assert_eq!(total(&r), 55);
    }
}
