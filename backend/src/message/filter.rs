use super::{Encoding, MessageOut};

#[derive(Debug, Clone)]
pub enum Filter {
    KeyEquals(String),
    KeyContains(String),
    ValueContains(String),
    JsonPathEquals { path: String, value: String },
    /// Case-insensitive contains on key text OR value text — the bare-text
    /// filter box of the timeline design (`filter=contains&q=...`). The
    /// value side keeps the same decode-error exclusion as `ValueContains`
    /// (a base64-of-raw-bytes blob is not real content); the key side
    /// matches whenever the key decoded at all (a `None` key never matches,
    /// same convention as `KeyEquals`/`KeyContains`). Holds the needle
    /// already lower-cased at parse time (`Filter::parse`), not the raw
    /// query text — `matches` is called once per decoded record in the
    /// merge loop, so lower-casing here once beats re-lowering on every
    /// record.
    Contains(String),
}

impl Filter {
    pub fn parse(kind: &str, q: &str, path: Option<&str>) -> Result<Filter, String> {
        match kind {
            "key_eq" => Ok(Filter::KeyEquals(q.to_lowercase())),
            "key_contains" => Ok(Filter::KeyContains(q.to_lowercase())),
            "value_contains" => Ok(Filter::ValueContains(q.to_lowercase())),
            "contains" => Ok(Filter::Contains(q.to_lowercase())),
            "json_eq" => path
                .map(|p| Filter::JsonPathEquals { path: p.to_string(), value: q.to_lowercase() })
                .ok_or_else(|| "filter json_eq requires a path parameter".to_string()),
            other => Err(format!("unknown filter kind '{other}'")),
        }
    }
}

fn json_at_path<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for seg in path.split('.') {
        current = match current {
            serde_json::Value::Object(map) => map.get(seg)?,
            serde_json::Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

fn scalar_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.to_lowercase()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Null => Some("null".to_string()),
        _ => None,
    }
}

pub fn matches(filter: &Filter, msg: &MessageOut) -> bool {
    match filter {
        Filter::KeyEquals(q) => msg.key.as_ref().is_some_and(|k| k.text.to_lowercase() == *q),
        Filter::KeyContains(q) => msg.key.as_ref().is_some_and(|k| k.text.to_lowercase().contains(q.as_str())),
        // A value that failed to decode has no real content to search — its
        // `text` is just the base64 of the raw bytes, so matching against it
        // would produce false hits with no relationship to actual content.
        Filter::ValueContains(q) => msg.value.as_ref().is_some_and(|v| {
            v.encoding != Encoding::DecodeError && v.text.to_lowercase().contains(q.as_str())
        }),
        Filter::JsonPathEquals { path, value } => msg.value.as_ref().is_some_and(|v| {
            v.encoding != Encoding::DecodeError
                && serde_json::from_str::<serde_json::Value>(&v.text)
                    .ok()
                    .and_then(|root| json_at_path(&root, path).and_then(scalar_text))
                    .is_some_and(|found| found == *value)
        }),
        Filter::Contains(q) => {
            let key_hit = msg.key.as_ref().is_some_and(|k| k.text.to_lowercase().contains(q.as_str()));
            let value_hit = msg.value.as_ref().is_some_and(|v| {
                v.encoding != Encoding::DecodeError && v.text.to_lowercase().contains(q.as_str())
            });
            key_hit || value_hit
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{DecodedPayload, Encoding, MessageOut};

    fn msg(key: Option<&str>, value: &str, encoding: Encoding) -> MessageOut {
        MessageOut {
            partition: 0, offset: 0, timestamp_ms: Some(1),
            key: key.map(|k| DecodedPayload { encoding: Encoding::Utf8, text: k.into(), schema_id: None, error: None }),
            value: Some(DecodedPayload { encoding, text: value.into(), schema_id: None, error: None }),
            headers: vec![],
        }
    }

    #[test]
    fn key_filters() {
        let m = msg(Some("order-42"), "x", Encoding::Utf8);
        assert!(matches(&Filter::KeyEquals("order-42".into()), &m));
        assert!(!matches(&Filter::KeyEquals("order-4".into()), &m));
        assert!(matches(&Filter::KeyContains("er-4".into()), &m));
        assert!(!matches(&Filter::KeyContains("nope".into()), &m));
        assert!(!matches(&Filter::KeyEquals("x".into()), &msg(None, "x", Encoding::Utf8))); // null key never key-matches
    }

    #[test]
    fn value_contains() {
        let m = msg(None, "hello kafka world", Encoding::Utf8);
        assert!(matches(&Filter::ValueContains("kafka".into()), &m));
        assert!(!matches(&Filter::ValueContains("rabbit".into()), &m));
    }

    /// A value that failed to decode is stored as the raw
    /// payload's base64 `text` under `Encoding::DecodeError`. Matching
    /// content filters against that base64 blob produces false matches with
    /// no relationship to the actual (undecodable) content — per plan
    /// semantics, a decode-failed value matches NO content filter.
    #[test]
    fn value_contains_never_matches_decode_error() {
        let m = msg(None, "a-base64-blob-that-happens-to-contain-kafka", Encoding::DecodeError);
        assert!(!matches(&Filter::ValueContains("kafka".into()), &m));
    }

    #[test]
    fn json_path_never_matches_decode_error() {
        // Base64 text that also happens to be valid JSON shaped like the path expects.
        let m = msg(None, r#"{"a":1}"#, Encoding::DecodeError);
        let f = Filter::JsonPathEquals { path: "a".into(), value: "1".into() };
        assert!(!matches(&f, &m));
    }

    #[test]
    fn json_path_descends_objects_and_arrays() {
        let m = msg(None, r#"{"payload":{"user":{"id":42},"tags":["a","b"]}}"#, Encoding::Json);
        let f = Filter::JsonPathEquals { path: "payload.user.id".into(), value: "42".into() };
        assert!(matches(&f, &m));
        let f = Filter::JsonPathEquals { path: "payload.tags.1".into(), value: "b".into() };
        assert!(matches(&f, &m));
        let f = Filter::JsonPathEquals { path: "payload.user.id".into(), value: "43".into() };
        assert!(!matches(&f, &m));
        let f = Filter::JsonPathEquals { path: "payload.missing".into(), value: "42".into() };
        assert!(!matches(&f, &m));
    }

    #[test]
    fn json_path_on_non_json_value_never_matches() {
        let m = msg(None, "plain text", Encoding::Utf8);
        assert!(!matches(&Filter::JsonPathEquals { path: "a".into(), value: "1".into() }, &m));
    }

    #[test]
    fn parse_validates() {
        assert!(matches!(Filter::parse("key_eq", "k", None), Ok(Filter::KeyEquals(_))));
        assert!(matches!(Filter::parse("json_eq", "42", Some("a.b")), Ok(Filter::JsonPathEquals { .. })));
        assert!(matches!(Filter::parse("contains", "x", None), Ok(Filter::Contains(_))));
        assert!(Filter::parse("json_eq", "42", None).is_err());
        assert!(Filter::parse("sideways", "x", None).is_err());
    }

    #[test]
    fn key_filters_are_case_insensitive() {
        let m = msg(Some("Order-42"), "x", Encoding::Utf8);
        assert!(matches(&Filter::parse("key_eq", "ORDER-42", None).unwrap(), &m));
        assert!(matches(&Filter::parse("key_contains", "oRdEr", None).unwrap(), &m));
        assert!(!matches(&Filter::parse("key_eq", "order-4", None).unwrap(), &m));
    }

    #[test]
    fn value_contains_is_case_insensitive() {
        let m = msg(None, "Hello KAFKA World", Encoding::Utf8);
        assert!(matches(&Filter::parse("value_contains", "kafka", None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_contains", "rabbit", None).unwrap(), &m));
    }

    #[test]
    fn json_eq_is_case_insensitive_on_strings_exact_on_numbers() {
        let m = msg(None, r#"{"user":{"name":"Alice","id":42}}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_eq", "ALICE", Some("user.name")).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "42", Some("user.id")).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_eq", "43", Some("user.id")).unwrap(), &m));
    }

    #[test]
    fn contains_hits_on_key() {
        let m = msg(Some("order-42"), "irrelevant value", Encoding::Utf8);
        assert!(matches(&Filter::Contains("order".into()), &m));
        assert!(!matches(&Filter::Contains("nope".into()), &m));
    }

    #[test]
    fn contains_hits_on_value() {
        let m = msg(None, "hello kafka world", Encoding::Utf8);
        assert!(matches(&Filter::Contains("kafka".into()), &m));
        assert!(!matches(&Filter::Contains("rabbit".into()), &m));
    }

    #[test]
    fn contains_is_case_insensitive() {
        // Built via `Filter::parse`, like every real call site — `Contains`
        // holds its needle already lower-cased (see its own doc comment), so
        // a query built any other way isn't guaranteed to be case-insensitive.
        let m = msg(Some("Order-42"), "Hello KAFKA World", Encoding::Utf8);
        assert!(matches(&Filter::parse("contains", "ORDER", None).unwrap(), &m));
        assert!(matches(&Filter::parse("contains", "kafka", None).unwrap(), &m));
    }

    #[test]
    fn contains_never_matches_decode_error_value() {
        let m = msg(None, "a-base64-blob-that-happens-to-contain-special", Encoding::DecodeError);
        assert!(!matches(&Filter::Contains("special".into()), &m));
    }
}
