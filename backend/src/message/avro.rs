use apache_avro::{from_avro_datum, Schema};

pub fn decode(schema_json: &str, datum: &[u8]) -> Result<String, String> {
    let schema = Schema::parse_str(schema_json).map_err(|e| format!("avro schema parse: {e}"))?;
    let value = from_avro_datum(&schema, &mut &datum[..], None).map_err(|e| format!("avro decode: {e}"))?;
    let json: serde_json::Value = serde_json::Value::try_from(value)
        .map_err(|e| format!("avro to json: {e}"))?;
    serde_json::to_string(&json).map_err(|e| format!("json render: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::{Record, Value};
    use apache_avro::{to_avro_datum, Schema};

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
        let datum = to_avro_datum(&schema, record).unwrap();

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
