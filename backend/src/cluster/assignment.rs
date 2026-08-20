//! Decoder for the classic ConsumerProtocol `MemberAssignment` blob that the
//! group list returns per live member. Only the topic NAMES are read: they
//! decide which groups a topic's tab must inspect. Per-partition ownership
//! comes from the coordinator instead (`ffi::describe_consumer_group_blocking`),
//! which is right for the KIP-848 protocol too — this blob is empty there.
//!
//! Wire layout (big-endian, stable since Kafka 0.9):
//!   int16 version
//!   int32 topic_count, then per topic:
//!     int16 name_len + utf8 name
//!     int32 partition_count + partition_count × int32
//!   (trailing user-data bytes ignored)
//!
//! Any structural violation returns `None`: the caller MUST treat an
//! undecodable member as "could be consuming anything" and inspect the
//! group's committed offsets instead of trusting a guess (owner ruling
//! 2026-08-19: uncertainty degrades toward an extra call, never toward a
//! hidden row).

pub fn assigned_topics(buf: &[u8]) -> Option<Vec<String>> {
    let mut r = Reader { buf, pos: 0 };
    let version = r.i16()?;
    if version < 0 {
        return None;
    }
    let topic_count = r.i32()?;
    if topic_count < 0 {
        return None;
    }
    let mut topics = Vec::with_capacity(topic_count.min(64) as usize);
    for _ in 0..topic_count {
        let name_len = r.i16()?;
        if name_len < 0 {
            return None;
        }
        let name = std::str::from_utf8(r.bytes(name_len as usize)?).ok()?;
        let partition_count = r.i32()?;
        if partition_count < 0 {
            return None;
        }
        // checked: a corrupt count must fail the decode, never overflow the
        // skip length (which would wrap on a 32-bit target)
        r.bytes((partition_count as usize).checked_mul(4)?)?;
        topics.push(name.to_string());
    }
    Some(topics)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let out = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }

    fn i16(&mut self) -> Option<i16> {
        Some(i16::from_be_bytes(self.bytes(2)?.try_into().ok()?))
    }

    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_be_bytes(self.bytes(4)?.try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(version: i16, topics: &[(&str, &[i32])], userdata: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend(version.to_be_bytes());
        b.extend((topics.len() as i32).to_be_bytes());
        for (name, partitions) in topics {
            b.extend((name.len() as i16).to_be_bytes());
            b.extend(name.as_bytes());
            b.extend((partitions.len() as i32).to_be_bytes());
            for p in *partitions {
                b.extend(p.to_be_bytes());
            }
        }
        b.extend(userdata);
        b
    }

    #[test]
    fn decodes_single_and_multi_topic_assignments() {
        let single = blob(0, &[("demo-orders", &[0, 1, 2])], &[]);
        assert_eq!(assigned_topics(&single), Some(vec!["demo-orders".into()]));

        let multi = blob(1, &[("demo-orders", &[2]), ("demo-users", &[0, 1])], &[]);
        assert_eq!(
            assigned_topics(&multi),
            Some(vec!["demo-orders".into(), "demo-users".into()])
        );
    }




    #[test]
    fn trailing_userdata_is_ignored() {
        let b = blob(0, &[("t", &[0])], &[0xff, 0x00, 0x42, 0x01]);
        assert_eq!(assigned_topics(&b), Some(vec!["t".into()]));
    }

    #[test]
    fn empty_assignment_decodes_to_no_topics() {
        // a member mid-rebalance can hold a valid blob with zero topics
        let b = blob(0, &[], &[]);
        assert_eq!(assigned_topics(&b), Some(vec![]));
    }

    #[test]
    fn structural_violations_are_undecodable_not_guesses() {
        assert_eq!(assigned_topics(&[]), None, "empty buffer");
        assert_eq!(assigned_topics(&[0x00]), None, "truncated version");
        let negative_version = blob(-1, &[], &[]);
        assert_eq!(assigned_topics(&negative_version), None);

        let mut truncated_name = blob(0, &[("demo-orders", &[0])], &[]);
        truncated_name.truncate(9); // cut inside the topic name
        assert_eq!(assigned_topics(&truncated_name), None);

        // topic_count says one more entry than the buffer holds
        let mut lying_count = Vec::new();
        lying_count.extend(0i16.to_be_bytes());
        lying_count.extend(2i32.to_be_bytes());
        lying_count.extend(1i16.to_be_bytes());
        lying_count.extend(b"t");
        lying_count.extend(0i32.to_be_bytes());
        assert_eq!(assigned_topics(&lying_count), None);

        let mut bad_utf8 = blob(0, &[("xx", &[])], &[]);
        bad_utf8[8] = 0xff; // corrupt a name byte
        bad_utf8[9] = 0xfe;
        assert_eq!(assigned_topics(&bad_utf8), None);
    }

    /// A wildly oversized partition count must decode to `None` (and be
    /// checked, not multiplied blindly into a wrapped skip length).
    #[test]
    fn absurd_partition_count_is_rejected_without_overflow() {
        let mut b = Vec::new();
        b.extend(0i16.to_be_bytes());
        b.extend(1i32.to_be_bytes());
        b.extend(1i16.to_be_bytes());
        b.extend(b"t");
        b.extend(i32::MAX.to_be_bytes());
        assert_eq!(assigned_topics(&b), None);
    }
}
