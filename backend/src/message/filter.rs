use super::MessageOut;

#[derive(Debug, Clone)]
pub enum Filter {
    KeyEquals(String),
    KeyContains(String),
    ValueContains(String),
    JsonPathEquals { path: String, value: String },
}

impl Filter {
    pub fn parse(kind: &str, q: &str, path: Option<&str>) -> Result<Filter, String> {
        match kind {
            "key_eq" => Ok(Filter::KeyEquals(q.to_string())),
            "key_contains" => Ok(Filter::KeyContains(q.to_string())),
            "value_contains" => Ok(Filter::ValueContains(q.to_string())),
            "json_eq" => path
                .map(|p| Filter::JsonPathEquals { path: p.to_string(), value: q.to_string() })
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

fn scalar_eq(v: &serde_json::Value, expected: &str) -> bool {
    match v {
        serde_json::Value::String(s) => s == expected,
        serde_json::Value::Number(n) => n.to_string() == expected,
        serde_json::Value::Bool(b) => b.to_string() == expected,
        serde_json::Value::Null => expected == "null",
        _ => false,
    }
}

pub fn matches(filter: &Filter, msg: &MessageOut) -> bool {
    match filter {
        Filter::KeyEquals(q) => msg.key.as_ref().is_some_and(|k| k.text == *q),
        Filter::KeyContains(q) => msg.key.as_ref().is_some_and(|k| k.text.contains(q.as_str())),
        Filter::ValueContains(q) => msg.value.as_ref().is_some_and(|v| v.text.contains(q.as_str())),
        Filter::JsonPathEquals { path, value } => msg.value.as_ref().is_some_and(|v| {
            serde_json::from_str::<serde_json::Value>(&v.text)
                .ok()
                .and_then(|root| json_at_path(&root, path).map(|found| scalar_eq(found, value)))
                .unwrap_or(false)
        }),
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
        assert!(Filter::parse("json_eq", "42", None).is_err());
        assert!(Filter::parse("sideways", "x", None).is_err());
    }
}
