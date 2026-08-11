use super::{DecodedPayload, Encoding};
use base64::Engine as _;

pub fn confluent_schema_id(bytes: &[u8]) -> Option<i32> {
    (bytes.len() >= 5 && bytes[0] == 0)
        .then(|| i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]))
}

pub fn decode_plain(bytes: &[u8]) -> DecodedPayload {
    if let Ok(text) = std::str::from_utf8(bytes) {
        if serde_json::from_str::<serde_json::Value>(text).is_ok() {
            return DecodedPayload { encoding: Encoding::Json, text: text.to_string(), schema_id: None, error: None };
        }
        return DecodedPayload { encoding: Encoding::Utf8, text: text.to_string(), schema_id: None, error: None };
    }
    DecodedPayload {
        encoding: Encoding::Bytes,
        text: base64::engine::general_purpose::STANDARD.encode(bytes),
        schema_id: None,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Encoding;

    #[test]
    fn confluent_header_detected() {
        let bytes = [0u8, 0, 0, 0, 42, b'x'];
        assert_eq!(confluent_schema_id(&bytes), Some(42));
    }

    #[test]
    fn confluent_header_rejects_wrong_magic_or_short() {
        assert_eq!(confluent_schema_id(&[1u8, 0, 0, 0, 42, b'x']), None);
        assert_eq!(confluent_schema_id(&[0u8, 0, 0]), None);
    }

    #[test]
    fn valid_json_decodes_as_json() {
        let p = decode_plain(br#"{"user":{"id":42}}"#);
        assert_eq!(p.encoding, Encoding::Json);
        assert_eq!(p.text, r#"{"user":{"id":42}}"#);
        assert!(p.error.is_none());
    }

    #[test]
    fn plain_text_decodes_as_utf8() {
        let p = decode_plain(b"hello kafka");
        assert_eq!(p.encoding, Encoding::Utf8);
        assert_eq!(p.text, "hello kafka");
    }

    #[test]
    fn junk_json_prefix_is_not_json() {
        // "{oops" is utf8 but not valid JSON
        let p = decode_plain(b"{oops");
        assert_eq!(p.encoding, Encoding::Utf8);
    }

    #[test]
    fn binary_decodes_as_base64_bytes() {
        let p = decode_plain(&[0xff, 0xfe, 0x00, 0x01]);
        assert_eq!(p.encoding, Encoding::Bytes);
        assert_eq!(p.text, "//4AAQ==");
    }
}
