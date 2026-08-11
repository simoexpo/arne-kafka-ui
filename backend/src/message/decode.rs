use super::{DecodedPayload, Encoding};
use base64::Engine as _;
use crate::message::schema_registry::{SchemaRegistry, SchemaType};
use crate::message::{avro, proto};

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

fn decode_error(bytes: &[u8], schema_id: Option<i32>, error: String) -> DecodedPayload {
    DecodedPayload {
        encoding: Encoding::DecodeError,
        text: base64::engine::general_purpose::STANDARD.encode(bytes),
        schema_id,
        error: Some(error),
    }
}

pub async fn decode_payload(bytes: Option<&[u8]>, sr: Option<&SchemaRegistry>) -> Option<DecodedPayload> {
    let bytes = bytes?;
    let Some(schema_id) = confluent_schema_id(bytes) else {
        return Some(decode_plain(bytes));
    };
    let Some(sr) = sr else {
        return Some(decode_error(bytes, Some(schema_id),
            format!("message carries schema id {schema_id} but no schema registry is configured for this cluster")));
    };
    let schema = match sr.schema(schema_id).await {
        Ok(s) => s,
        Err(e) => return Some(decode_error(bytes, Some(schema_id), e)),
    };
    let body = &bytes[5..];
    let result = match schema.schema_type {
        SchemaType::Avro => avro::decode(&schema.schema, body).map(|text| (Encoding::Avro, text)),
        SchemaType::Protobuf => proto::decode(&schema.schema, body).map(|text| (Encoding::Protobuf, text)),
        SchemaType::Json => {
            let plain = decode_plain(body);
            // `decode_plain` labels non-JSON bodies Utf8/Bytes; only trust
            // the registry's declared JSON schema type when the body
            // actually parsed as JSON, otherwise this would silently stamp
            // `Encoding::Json` on a payload that plainly isn't.
            if plain.encoding == Encoding::Json {
                Ok((Encoding::Json, plain.text))
            } else {
                Err(format!(
                    "schema registry declares schema id {schema_id} as JSON but the payload body is not valid JSON"
                ))
            }
        }
    };
    Some(match result {
        Ok((encoding, text)) => DecodedPayload { encoding, text, schema_id: Some(schema_id), error: None },
        Err(e) => decode_error(bytes, Some(schema_id), e),
    })
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

    use crate::message::schema_registry::SchemaRegistry;

    #[tokio::test]
    async fn none_bytes_is_none() {
        assert!(decode_payload(None, None).await.is_none());
    }

    #[tokio::test]
    async fn header_without_registry_is_decode_error_with_raw() {
        let bytes = [0u8, 0, 0, 0, 7, 1, 2, 3];
        let p = decode_payload(Some(&bytes), None).await.unwrap();
        assert_eq!(p.encoding, Encoding::DecodeError);
        assert_eq!(p.schema_id, Some(7));
        assert!(p.error.unwrap().contains("no schema registry"));
        assert!(!p.text.is_empty()); // base64 of the raw bytes
    }

    #[tokio::test]
    async fn no_header_falls_through_to_plain() {
        let p = decode_payload(Some(b"{\"a\":1}"), None).await.unwrap();
        assert_eq!(p.encoding, Encoding::Json);
    }

    #[tokio::test]
    async fn json_schema_type_with_valid_json_body_decodes_as_json() {
        let sr = SchemaRegistry::new(&crate::message::schema_registry::tests::mock_sr_for_decode().await);
        let mut bytes = vec![0u8, 0, 0, 0, 9];
        bytes.extend_from_slice(br#"{"a":1}"#);
        let p = decode_payload(Some(&bytes), Some(&sr)).await.unwrap();
        assert_eq!(p.encoding, Encoding::Json);
        assert_eq!(p.text, r#"{"a":1}"#);
        assert_eq!(p.schema_id, Some(9));
    }

    /// I4 regression: the schema registry declares schema id 9 as JSON, but
    /// the actual payload body is not valid JSON. Stamping `Encoding::Json`
    /// on it anyway (as `decode_plain` would label it `Utf8`/`Bytes`)
    /// silently mislabels the payload; it must surface as a decode error.
    #[tokio::test]
    async fn json_schema_type_with_invalid_json_body_is_decode_error() {
        let sr = SchemaRegistry::new(&crate::message::schema_registry::tests::mock_sr_for_decode().await);
        let mut bytes = vec![0u8, 0, 0, 0, 9];
        bytes.extend_from_slice(b"not json at all");
        let p = decode_payload(Some(&bytes), Some(&sr)).await.unwrap();
        assert_eq!(p.encoding, Encoding::DecodeError);
        assert_eq!(p.schema_id, Some(9));
        assert!(p.error.as_ref().unwrap().contains("JSON"), "got: {:?}", p.error);
        assert!(!p.text.is_empty());
    }

    #[tokio::test]
    async fn avro_via_registry_decodes() {
        // mock SR serving schema id 7 as avro "string"; datum "hi" = [0x04, b'h', b'i']
        let sr = SchemaRegistry::new(&crate::message::schema_registry::tests::mock_sr_for_decode().await);
        let mut bytes = vec![0u8, 0, 0, 0, 7];
        bytes.extend_from_slice(&[0x04, b'h', b'i']);
        let p = decode_payload(Some(&bytes), Some(&sr)).await.unwrap();
        assert_eq!(p.encoding, Encoding::Avro);
        assert_eq!(p.text, "\"hi\"");
        assert_eq!(p.schema_id, Some(7));
    }
}
