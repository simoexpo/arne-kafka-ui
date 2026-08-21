use protobuf::reflect::FileDescriptor;
use std::sync::atomic::{AtomicU64, Ordering};

static CALL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Removes its directory (recursively) when dropped, so early `?` returns
/// in `decode` still clean up the per-call temp dir.
struct TempDirGuard(std::path::PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn read_zigzag_varint(bytes: &[u8]) -> Result<(i64, &[u8]), String> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, b) in bytes.iter().enumerate() {
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            let decoded = ((result >> 1) as i64) ^ -((result & 1) as i64);
            return Ok((decoded, &bytes[i + 1..]));
        }
        shift += 7;
        if shift >= 64 {
            break;
        }
    }
    Err("truncated varint in message indexes".into())
}

const MAX_MESSAGE_INDEX_COUNT: i64 = 128;

pub fn read_message_indexes(bytes: &[u8]) -> Result<(Vec<i32>, &[u8]), String> {
    let (count, mut rest) = read_zigzag_varint(bytes)?;
    if count == 0 {
        return Ok((vec![0], rest));
    }
    if !(0..=MAX_MESSAGE_INDEX_COUNT).contains(&count) {
        return Err(format!(
            "invalid message index count {count} (must be between 0 and {MAX_MESSAGE_INDEX_COUNT})"
        ));
    }
    let mut indexes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (idx, r) = read_zigzag_varint(rest)?;
        indexes.push(idx as i32);
        rest = r;
    }
    Ok((indexes, rest))
}

/// Parses `.proto` source text into a `FileDescriptor`. This is the
/// expensive part of protobuf decoding — it shells out to `protobuf-parse`
/// via a temp file and rebuilds the descriptor from scratch — so callers
/// that decode many messages against the same schema id should parse once
/// and reuse the result via `decode_with_descriptor` (see
/// `SchemaRegistry::parsed`'s cache), rather than calling this per message.
/// The first message's package-qualified name — the subject itself under
/// the record-name strategy for protobuf schemas. `None` when the source
/// doesn't parse or declares no message. Multi-message files use the
/// first declaration, the registry's own convention.
pub fn message_fqn(proto_src: &str) -> Option<String> {
    let fd = build_descriptor(proto_src).ok()?;
    let message = fd.messages().next()?;
    let package = fd.proto().package();
    if package.is_empty() {
        Some(message.name().to_string())
    } else {
        Some(format!("{package}.{}", message.name()))
    }
}

pub fn build_descriptor(proto_src: &str) -> Result<FileDescriptor, String> {
    // protobuf-parse reads files from disk; write the SR schema text to a temp file
    let call_id = CALL_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arne-proto-{}-{}", std::process::id(), call_id));
    std::fs::create_dir_all(&dir).map_err(|e| format!("tmp dir: {e}"))?;
    let _dir_guard = TempDirGuard(dir.clone());
    let path = dir.join("schema.proto");
    std::fs::write(&path, proto_src).map_err(|e| format!("tmp write: {e}"))?;

    let parsed = protobuf_parse::Parser::new()
        .pure()
        .include(&dir)
        .input(&path)
        .parse_and_typecheck()
        .map_err(|e| format!("proto parse: {e}"))?;

    let fd_proto = parsed
        .file_descriptors
        .into_iter()
        .find(|fd| {
            fd.name
                .as_deref()
                .map(|n| n.ends_with("schema.proto"))
                .unwrap_or(false)
        })
        .ok_or("proto parse produced no descriptor")?;
    FileDescriptor::new_dynamic(fd_proto, &[]).map_err(|e| format!("descriptor: {e}"))
}

/// Decodes `payload_after_header` (the confluent-framed body past the
/// magic byte + schema id + message indexes) using an already-parsed
/// `FileDescriptor`. Cheap: no re-parsing of the `.proto` source.
pub fn decode_with_descriptor(
    fd: &FileDescriptor,
    payload_after_header: &[u8],
) -> Result<String, String> {
    let (indexes, body) = read_message_indexes(payload_after_header)?;
    if indexes != [0] {
        return Err(format!(
            "unsupported protobuf message index path {indexes:?} (v1 decodes only the first top-level message)"
        ));
    }
    let md = fd
        .messages()
        .next()
        .ok_or("proto schema declares no message")?;
    let msg = md
        .parse_from_bytes(body)
        .map_err(|e| format!("protobuf decode: {e}"))?;
    protobuf_json_mapping::print_to_string(&*msg).map_err(|e| format!("proto to json: {e}"))
}

/// Parses `proto_src` and decodes `payload_after_header` in one call.
/// Test-only convenience: the schema registry decode path uses
/// `build_descriptor` + `decode_with_descriptor` separately so it can cache
/// the descriptor per schema id.
#[cfg(test)]
pub fn decode(proto_src: &str, payload_after_header: &[u8]) -> Result<String, String> {
    let fd = build_descriptor(proto_src)?;
    decode_with_descriptor(&fd, payload_after_header)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROTO: &str = r#"syntax = "proto3";
message User {
  int64 id = 1;
  string name = 2;
}
"#;

    /// The first message's fully qualified name (package-qualified) IS the
    /// subject under the record-name strategy for protobuf schemas.
    #[test]
    fn message_fqn_is_the_package_qualified_first_message() {
        assert_eq!(message_fqn(PROTO), Some("User".to_string()));
        let with_pkg = r#"syntax = "proto3"; package com.acme; message Event { int64 id = 1; } message Other { int64 x = 1; }"#;
        assert_eq!(message_fqn(with_pkg), Some("com.acme.Event".to_string()));
        assert_eq!(message_fqn("not a proto"), None);
    }

    // field 1 (varint) = 42, field 2 (len-delimited) = "ada"
    const USER_BYTES: &[u8] = &[0x08, 0x2a, 0x12, 0x03, b'a', b'd', b'a'];

    #[test]
    fn zero_byte_means_first_message() {
        let (idx, rest) = read_message_indexes(&[0x00, 0xAA]).unwrap();
        assert_eq!(idx, vec![0]);
        assert_eq!(rest, &[0xAA]);
    }

    #[test]
    fn negative_index_count_is_error() {
        assert!(read_message_indexes(&[0x01, 0x00]).is_err());
    }

    #[test]
    fn message_index_count_above_the_max_is_error() {
        // count=129 (zigzag 129*2=258, varint-encoded as [0x82, 0x02]) — one
        // past MAX_MESSAGE_INDEX_COUNT (128), the upper-bound guard that
        // count==0/negative-count coverage above doesn't exercise.
        let err = read_message_indexes(&[0x82, 0x02]).unwrap_err();
        assert!(err.contains("129") && err.contains("128"), "got: {err}");
    }

    #[test]
    fn explicit_indexes_are_parsed() {
        // count=1 (zigzag 1 = 0x02), index=1 (zigzag 1 = 0x02)
        let (idx, rest) = read_message_indexes(&[0x02, 0x02, 0xBB]).unwrap();
        assert_eq!(idx, vec![1]);
        assert_eq!(rest, &[0xBB]);
    }

    #[test]
    fn decodes_first_message_to_json() {
        let mut payload = vec![0x00];
        payload.extend_from_slice(USER_BYTES);
        let json = decode(PROTO, &payload).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["name"], "ada");
        assert_eq!(v["id"].as_str().unwrap_or_default(), "42"); // proto3 JSON prints int64 as string
    }

    #[test]
    fn non_first_message_index_is_a_clear_error() {
        let err = decode(PROTO, &[0x02, 0x02, 0x08, 0x2a]).unwrap_err();
        assert!(err.contains("message index"), "got: {err}");
    }

    #[test]
    fn garbage_proto_source_is_error() {
        assert!(decode("not proto at all {{{", &[0x00]).is_err());
    }
}
