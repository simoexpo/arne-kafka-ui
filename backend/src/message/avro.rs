use apache_avro::Schema;

/// Parses an Avro schema JSON string. This is the expensive part of Avro
/// decoding, so callers that decode many messages against the same schema
/// id should parse once and reuse the result via `decode_with_schema` (see
/// `SchemaRegistry::parsed`'s cache), rather than calling this per message.
pub fn parse_schema(schema_json: &str) -> Result<Schema, String> {
    Schema::parse_str(schema_json).map_err(|e| format!("avro schema parse: {e}"))
}

/// Decodes `datum` (the confluent-framed body past the magic byte + schema
/// id) using an already-parsed `Schema`. Cheap: no re-parsing of the schema
/// JSON.
pub fn decode_with_schema(schema: &Schema, datum: &[u8]) -> Result<String, String> {
    // TODO: migrate to GenericDatumReader (tracked in plan 2 follow-ups)
    #[allow(deprecated)]
    let value = apache_avro::from_avro_datum(schema, &mut &datum[..], None).map_err(|e| format!("avro decode: {e}"))?;
    let json: serde_json::Value = serde_json::Value::try_from(value)
        .map_err(|e| format!("avro to json: {e}"))?;
    serde_json::to_string(&json).map_err(|e| format!("json render: {e}"))
}

/// Parses `schema_json` and decodes `datum` in one call. Test-only
/// convenience: the schema registry decode path uses `parse_schema` +
/// `decode_with_schema` separately so it can cache the parsed schema per
/// schema id.
#[cfg(test)]
pub fn decode(schema_json: &str, datum: &[u8]) -> Result<String, String> {
    let schema = parse_schema(schema_json)?;
    decode_with_schema(&schema, datum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::{Record, Value};
    use apache_avro::Schema;

    const SCHEMA: &str = r#"{
        "type": "record", "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    #[test]
    fn decodes_record_datum_to_json() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put("id", Value::Long(42));
        record.put("name", Value::String("ada".into()));
        // test-only fixture encoder; production code never writes Avro.
        #[allow(deprecated)]
        let datum = apache_avro::to_avro_datum(&schema, record).unwrap();

        let json = decode(SCHEMA, &datum).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["id"], 42);
        assert_eq!(v["name"], "ada");
    }

    #[test]
    fn bad_schema_is_error() {
        assert!(decode("not a schema", &[0]).is_err());
    }

    #[test]
    fn truncated_datum_is_error() {
        let err = decode(SCHEMA, &[0x02]).unwrap_err();
        assert!(!err.is_empty());
    }
}
